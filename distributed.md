#Distributred queries in Mung

> "How do I give my analysts access to Big Data?"

The easiest solution is to get a bigger server and just run Postgres on that. Unfortunately that doesn't really scale to "big data", so we need a different way to seperate our data onto different machines so that we can combine the power of multiple machines.

The easiest way to do this is to put everything into Hadoop, then use one of the many SQL layers to request the data from the hadoop data nodes and run the SQL in a query engine over that.  We could use any of:

* Apache Spark
* Cloudera Impala
* Apache Hive
* Presto (running on Hive or HDFS)


## Problems with this

 * Slow. None of the solutions are as performant as a normal database
 * Expensive They all require a Hadoop Cluster, which if hosted on the cloud starts at 4 figures a month.
 
The central issue here appears to be that we rely on the query engine to be smart enough to handle all data access, which means that it can't take obvious shortcuts like using indexes.

## Lets run Postgres on Data Nodes

Lets say we have the following query:


```sql
SELECT COUNT(DISTINCT OrderId), SUM(O.Amount), COUNT(*) AS Items
FROM LineItem LI
INNER JOIN Order O
ON LI.OrderId = O.OrderId
INNER JOIN Customer C
ON O.CustomerId = C.CustomerID
WHERE C.Country = 'US'
AND		O.OrderDate BETWEEN '2013-01-01' AND '2014-12-30'
```

This is all fine and well to execute on a single server.  But lets say that we have a cluster of 4 servers that we have split the data on.  So our `connections.json` looks like:

```json
{
	[
    	{"name": "dbw-01", "provider": "porstgres", "connection_string": "<>"},
    	{"name": "dbw-02", "provider": "porstgres", "connection_string": "<>"},
    	{"name": "dbw-03", "provider": "porstgres", "connection_string": "<>"},
    	{"name": "dbw-04", "provider": "porstgres", "connection_string": "<>"}
    ]
}
```

Lets assume that each database server has 1/4 of the data for each of our 3 tables (`LineItem`, `Order`, and `Customer`).  Using the mung query context we could write a query that looks like:

```sql
SELECT COUNT(DISTINCT OrderId), SUM(O.Amount), COUNT(*) AS Items
FROM (
	@dbw-01(SELECT * FROM LineItem )
    UNION ALL @dbw-02(SELECT * FROM LineItem)
    UNION ALL @dbw-03(SELECT * FROM LineItem)
    UNION ALL @dbw-04(SELECT * FROM LineItem)
) LI
INNER JOIN (
	@dbw-01(SELECT * FROM @dbw-01.Order )
    UNION ALL @dbw-02(SELECT * FROM Order)
    UNION ALL @dbw-03(SELECT * FROM Order)
    UNION ALL @dbw-04(SELECT * FROM Order)
) O
ON LI.OrderId = O.OrderId
INNER JOIN Customer (
	@dbw-01(SELECT * FROM Customer)
    UNION ALL @dbw-02(SELECT * FROM Customer)
    UNION ALL @dbw-03(SELECT * FROM Customer)
    UNION ALL @dbw-04(SELECT * FROM Customer)
) C
ON O.CustomerId = C.CustomerID

WHERE C.Country = 'US'
AND		O.OrderDate BETWEEN '2013-01-01' AND '2014-12-30'

```

Now that would be pretty slow, since essentially we would be copying all the data from each of the 4 servers onto the server we chose to run this query on, and then executing the query over all the data on the one server. 

Despite that, this still gives us an important capability.  The syntax is a bit bulky though, so lets introduce a wildcard into the "@[connection]" command, so that mung will attempt to match the pattern against all its connections and union the result.  The query thus becomes:

```sql
SELECT COUNT(DISTINCT OrderId), SUM(O.Amount), SUM(Quantity) AS Items
FROM @dbw-*(SELECT * FROM LineItem) LI
INNER JOIN @dbw-*(SELECT * FROM @dbw-01.Order) O
ON LI.OrderId = O.OrderId
INNER JOIN @dbw-*(SELECT * FROM Customer) C
ON O.CustomerId = C.CustomerID
WHERE C.Country = 'US'
AND		O.OrderDate BETWEEN '2013-01-01' AND '2014-12-30'

```
	


We could be smarter and do the filtering within the inner queries, which would help make the query faster:


```sql
SELECT COUNT(DISTINCT OrderId), SUM(O.Amount), SUM(Quantity) AS Items
FROM @dbw-*(SELECT * FROM @dbw-01.LineItem) LI
INNER JOIN @dbw-*(
	SELECT * 
    FROM Order
    WHERE OrderDate BETWEEN '2013-01-01' AND '2014-12-30'
) O
ON LI.OrderId = O.OrderId
INNER JOIN @dbw-*(
	SELECT * 
    FROM Customer 
    WHERE Country = 'US'
) C
ON O.CustomerId = C.CustomerID
```

Now the query would be dramatically faster, since the filtering of data is completed on the postgres instance that contains the data.

We can still do better though, since the query still requires that we copy over all the LineItems in the database, rather than just those for the orders we care about (those in the US and which ocurred in 2013).

At this point, we can see that we can vastly improve performance if we can introduce data locality requirements.  such as:

* Each Order will have its LineItems on the same partition.  

If that is the case, then we can actually do the LineItem / Order join on each partition, so the query becomes:


```sql
SELECT SUM(Orders), SUM(O.Amount), SUM(O.Items) AS Items
FROM @dbw-*(
	SELECT COUNT(DISTINCT OrderId) As Orders, SUM(O.Amount) AS Amount, SUM(Quantity) AS Items, O.CustomerId
    FROM LineItem LI
    INNER JOIN Order O
    ON LI.OrderId = O.OrderId
    WHERE OrderDate BETWEEN '2013-01-01' AND '2014-12-30'
    GROUP BY CustomerId
) O
ON LI.OrderId = O.OrderId
INNER JOIN @dbw-*(
	SELECT * 
    FROM Customer 
    WHERE Country = 'US'
) C
ON O.CustomerId = C.CustomerID
```

Not only can we filter the LineItem table on each server, we can also do our aggregation on each of the data servers, before returning a record for each customer.  We can then join this pre-aggregated data on the complete customers set to see which of these customers are from the US.

Lets go a step further, and add another requirement, that the Customer table, rather than being partitioned, must be duplicated on each server.  That is, whenever a new new Customer is added, it is copied to ALL servers.  If this is the case, then we can further modify the query to be:

```sql
SELECT SUM(Orders), SUM(O.Amount), SUM(O.Count) AS Items
FROM @dbw-*(
	SELECT COUNT(DISTINCT OrderId) As Orders, SUM(O.Amount) AS Amount, SUM(Quantity) AS Items
    FROM LineItem LI
    INNER JOIN Order O
    ON LI.OrderId = O.OrderId
    INNER JOIN Customer C
    ON O.CustomerId = C.CustomerID
    WHERE OrderDate BETWEEN '2013-01-01' AND '2014-12-30'
    AND   C.Country = 'US'
) O
```

Here we see that the query can be satisfied with each partition only returning a single row, which can then be combined on the server the query is executed on.  This means that the vast majority of the work is done in parallel without any significant communication requirement.

## Defining data locality rules

Lets take the following data DDL to generate the tables used above.

```sql
CREATE TABLE Customer (
	CustomerId INT NOT NULL
    Name VARCHAR(100)
    Country VARCHAR(100),
    CONSTRAINT PK_Customer PRIMARY KEY (CustomerId) 
)

CREATE TABLE Order (
	OrderId INT NOT NULL,
    CustomerId INT NOT NULL,
    OrderDate TimeStamp NOT NULL,
    Amount DECIMAL NOT NULL,
    CONSTRAINT PK_Order PRIMARY KEY (OrderId),
    CONSTRAINT FK_Order_Customer FOREIGN KEY (CustomerId) REFERENCES Customer(CustomerId) 
)

CREATE TABLE LineItem (
	LineItemId INT NOT NULL,
    OrderId INT NOT NULL,
    Product VARCHAR(100),
    Quantity INT
    Amount DECIMAL NOT NULL,
    CONSTRAINT PK_LineItem PRIMARY KEY (LineItem),
    CONSTRAINT FK_LineItem_Order FOREIGN KEY (OrderId) REFERENCES Order(OrderId) 
)

```

With the addition of a bit of non-standard SQL we can tell mung how to distribute data in terms of locality.


```sql
CREATE TABLE Customer (
	CustomerId INT NOT NULL
    Name VARCHAR(100)
    Country VARCHAR(100),
    CONSTRAINT PK_Customer PRIMARY KEY (CustomerId) 
) WITH (MUNG_REQUIRECOPIES=ALL)
/* Insert into all partitions, only suceeding when all partitions have acknowledged the insert*/

CREATE TABLE Order (
	OrderId INT NOT NULL,
    CustomerId INT NOT NULL,
    OrderDate TimeStamp NOT NULL,
    Amount DECIMAL NOT NULL,
    CONSTRAINT PK_Order PRIMARY KEY (OrderId),
    CONSTRAINT FK_Order_Customer FOREIGN KEY (CustomerId) REFERENCES Customer(CustomerId) 
) WITH (MUNG_REQUIRECOPIES=1, MUNG_SELECT_PARTITION=RANDOM)
/* Insert into a single random partition, suceeding when one partition acknowledges success */

CREATE TABLE LineItem (
	LineItemId INT NOT NULL,
    OrderId INT NOT NULL,
    Product VARCHAR(100),
    Quantity INT
    Amount DECIMAL NOT NULL,
    CONSTRAINT PK_LineItem PRIMARY KEY (LineItem),
    CONSTRAINT FK_LineItem_Order FOREIGN KEY (OrderId) REFERENCES Order(OrderId) 
) WITH (MUNG_REQUIRECOPIES=1, MUNG_SELECT_PARTITION=ALL)
/* 
	Insert into all partitions, succeeding when a a single partition acknowledges success
	Since there is a foreign key defined, copies sent to partitions without the corresponding
    OrderId will fail because of the foreign key not being satisfied.  This guarantees us data
    locality as required.
*/

```

Thus with only a few minor changes to the schema, and to queries we can effectively distribute data across multiple data nodes, and have those data nodes efficiently and effectively query data in a native format without unneccessary network IO.
