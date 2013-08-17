#Mung

> "This data is in exactly the right format"
> -- <cite>No Data Scientist Ever</cite>   

Mung provides a query engine which rewrites and executes SQL against disparate databases, enabling the output from different queries to be merged and manipulated as if they were local expressions.

Mung removes the need to be constantly exporting and importing data as CSV files, enabling you to treat any data source as if it were local while still giving you control over where and how data is processed.

Mung also enables the partitioning of data so that any database can be split amongst many servers and queried as if it were a single database. 


## Example
Lets assume that an organization has the following systems:

- accounting (tracking invoices, payments etc - Oracle)
- web_events (tracking signups, sessions, etc - SQL Server)
- support (tickets, etc - Postgresql)

And your CEO says to you:

> "I want a list of all my clients who have generated more than $1M in revenue, that haven't been active in the last week but have been active in the last month, and which have at least one open support ticket that's more than 1 week old."


[connections.json]
```json
{
	"accounting": "Data Source=accounting_srv;User Id=***;Password=***;Integrated Security=no;",
    "web_events": "data source=web_srv;initial catalog=web_events;user id=***;password=***",
    "support": "Provider=PostgreSQL OLE DB Provider;Data Source=support_srv;location=support;User ID=***;Password=***;",
    "mung": "<Mung Data Warehouse Process / MonetDB>"
}

```
The connections.json file tells MUNG how to connect to the various data sources.  MUNG currently supports SQL Server, Oracle, MySql, Sqlite, Postgres, MariaDB, MonetDB, RedShift and many more. 


[example.mql]

```sql
@mung(

    SELECT high_value.company_name, high_value.email, high_value.revenue, inactive.LastSeen, open_tickets.tickets

    FROM @accounting(
        SELECT I.client_id, I.revenue, C.email, C.company_name, C.contact_name
        FROM (
            SELECT client_id, SUM(total_amount) as revenue, 
            FROM invoice I
            WHERE I.invoice_date > CURRENT_TIMESTAMP - 7
            GROUP BY client_id
            HAVING SUM(total_amount) > 1000000
        ) I
        INNER JOIN client C
        ON I.client_id = C.client_id
    ) high_value

    INNER JOIN  @web_events(
        SELECT U.CompanyName, MAX(SessionStart) as LastSeen
        FROM Sessions S
        INNER JOIN Users U
        ON U.UserId = S.UserId
        WHERE U.UserId IN (
            SELECT UserId
            FROM Sessions 
            WHERE SessionStart > DATEADD(DAY, -30, GETUTCDATE())
        )
        GROUP BY CompanyName
        HAVING MAX(SessionStart) < DATEADD(DAY, -7, GETUTCDATE()
    ) inactive
    ON high_value.company_name = inactive.CompanyName

    INNER JOIN @support(
        SELECT company, COUNT(*) tickets, MIN(open_date) AS first_opened
        FROM tickets T
        WHERE T.status = 'open'
        GROUP BY company
    ) open_tickets
    ON open_tickets.company = high_value.company_name

    ORDER BY high_value.revenue DESC
)

```

The `@<connection_name>(<query>)` specifies that `<query>` is to be executed on connection `<connection_name>`/.  The results of the query will automatically be streamed to the parent context, and deleted following query execution.
    

##Why Mung
I got sick of writing code to move data from one database to another.

So I decided to write code to move data from one database to another (smart eh?), but at least it's written now so you can avoid writing it!

##What can I use Mung for?

###Importing data from production systems
Operation data systems contain the data you need, but the queries we want to run as Data Scientists are often rather resource intensive.  The last thing we want is to break production systems.

In high end systems, database mirroring or replication is a good way to get around these issues, but that means extra hardware and more ways to break your production systems.  Often you only need to access a handful of tables, so why not just use:

```sql
@output(mung.invoices)
@cache(MAX_AGE=24H,LAST=2013-10-14T12:34AM)

@accounting(
	SELECT *
	FROM invoices
	WHERE invoice_id > @mung(
		SELECT MAX(invoice_id)
	    FROM invoices
	)
)
```

`@output(<connection>.<table>)` specifies that when this script is run, its output should streamed to the `<table>` using `<connection>`.  As with all MUNG queries, if the table does not exist, it will be created.  If the table does exist, the table will be appended to.

`@cache(CACHE_STRING)` tells MUNG about when to query the output table (`mung.invoices` in this case), or when to actually run the query.  This enables you to work with cached data rather than going back to source tables.    This means you can create cron jobs to run MUNG periodically to update queries.  The `LAST` field of the CACHE_STRING will be updated every time the execution of the query actually updates raw data.


### Precooking data
Analysis often requires complex aggregation over large data sources.  These queries are often expensive and thus not suitable for real time use in dashboards, reports and API's.

To get around this, most analysis is done on "cached" or "pre-cooked" transformations of data, so that queries can be answered in milliseconds rather than hours.

A rather contrived example may be: 

>Calculate the total invoice amount for incoices generated in the state of 'New York' in the 'United States' for that contain 'Professional Services' for the last 30 days

For use in a management dashboard, where the state and country can be changed dynamically (using the parameters `@country_code`, `@state_code` and `@product_code`:.

Thus the query might look like:

```sql
@accounting(
	SELECT SUM(amount) 
	FROM invoices i
	INNER JOIN invoice_lines l
	ON i.invoice_id = l.invoice_id
	INNER JOIN products p
	ON p.product_id = l.product_id
	WHERE  i.country = @country_code
	AND    l.state = @state_code
	AND    p.product_name = @product_code
	AND    i.invoice_date > CURRENT_TIMESTAMP - 30 
)
```

The issue is that the invoices table may have millions of rows in it, the invoice_lines table a multiple of that.  This makes the query too slow to be used in a web context.

The solution is to create a cached representation of the table with the expensive join and aggregation "pre-cooked".  The query to generate the cached representation then looks like:

[cached_transactions.mql]
```sql
@output(mung.cached_transactions, DROP)
@update_policy(MAX_AGE=24H,LAST=2013-10-14T12:01AM)

@accounting(
	SELECT SUM(amount) as amount
	FROM invoices i
	INNER JOIN invoice_lines l
	ON i.invoice_id = l.invoice_id
	INNER JOIN products p
	ON p.product_id = l.product_id
	GROUP BY i.country, i.state, l.product_name
)
```

From our management dashboard, we can then execute the following query each time the user requests the value for a `@country_code`, `@state_code` and `@product_code`:

[get_cached_transactions.mql]
```sql
SELECT amount 
FROM   @cached_transactions.mql
WHERE  country = @country_code
AND    state = @state_code
AND    product_name = @product_code

```

Since there is no aggregation or join, this query returns in milliseconds.

### Excution against a .mql file
You may have noticed that [get_cached_transactions.mql] references `@cached_transactions.mql`, rather than a table directly. 

This instructs MUNG to refer to the script file rather than sending the query directly to the database engine.  When this query is executed as follows:

    mung exec get_cached_transactions.mql @country_code='US',@state_code='NY',@product_code='Services'

Mung will go through the following steps:

- read the `cached_transactions.mql` file
- rewrite the `@cached_transactions.mql` reference to point to the `cached_transactions` table on the `mung` connection (as referenced in the `@output()` directive
- execute the re-written query using the `mung` connection

This will happen recursively, since a  `.mql` file may in turn reference other `.mql` files.   

### Refreshing of cached data
Queries can be updated by executing:

    mung refresh <query_name>.mql

When evaulating an update request, the `@update_policy()` directive will be evaluated to determine if the query neeeds to be updated.

Where query references other queries, they will also be evaluated, such that if a lower level query requires updating, it will force an update to all queries referencing it so as to maintain consistency.

For example:
[parent.mql]
```sql
@input(mung)
@output(mung.cached_transactions, DROP)
@update_policy(MAX_AGE=7D,LAST=2013-10-14T12:01AM)

SELECT * FROM child.mql
```

[child.mql]
```sql
@input(accounting)
@output(mung.cached_transactions, DROP)
@update_policy(MAX_AGE=1H,LAST=2013-10-14T12:01AM)

SELECT * FROM invoices
```

If at 2013-10-14:2:00AM we run:

    mung update parent.mql
    
MUNG will read `parent.mql` and see that it was last run 2 hours ago, which means it doesn't require an update.  However, it will also read `child.mql`, which only has a 1 hour MAX_AGE and thus needs to be re-run.  This will in turn force an update of `parent.mql`.

If the MAX_AGE's were reversed (parent has a MAX_AGE of 1H and child of 7D), then MUNG would update `parent.mql`, but not update `child.mql`


### Forcing updates of cached data
Queries may also be updated ignoring any `@update_policy` directive using the following command:

    mung update parent.mql







