
@mung-sqlite-*(

	CREATE TABLE customer(
	    CustKey INTEGER NULL,
	    Name TEXT NULL,
	    Address TEXT NULL,
	    NationKey INTEGER NULL,
	    Phone TEXT NULL,
	    AcctBal REAL NULL,
	    MktSegment TEXT NULL,
	    Comment TEXT NULL
	)
	
	CREATE TABLE lineitem(
	    OrderKey INTEGER NULL,
	    PartKey INTEGER NULL,
	    SuppKey INTEGER NULL,
	    LineNumber INTEGER NULL,
	    Quantity INTEGER NULL,
	    ExtendedPrice REAL NULL,
	    Discount REAL NULL,
	    Tax REAL NULL,
	    ReturnFlag TEXT NULL,
	    LineStatus TEXT NULL,
	    ShipDate datetime NULL,
	    CommitDate datetime NULL,
	    ReceiptDate datetime NULL,
	    ShipInstruct TEXT NULL,
	    ShipMode TEXT NULL,
	    Comment TEXT NULL
	)

	CREATE TABLE nation(
	    NationKey INTEGER NULL,
	    Name TEXT NULL,
	    RegionKey INTEGER NULL,
	    Comment varchar(160) NULL
	)

	CREATE TABLE orders(
	    OrderKey INTEGER NULL,
	    CustKey INTEGER NULL,
	    OrderStatus TEXT NULL,
	    TotalPrice REAL NULL,
	    OrderDate datetime NULL,
	    OrderPriority TEXT NULL,
	    Clerk TEXT NULL,
	    ShipPriority INTEGER NULL,
	    Comment TEXT NULL
	)

	CREATE TABLE part(
	    PartKey INTEGER NULL,
	    Name TEXT NULL,
	    Mfgr TEXT NULL,
	    Brand TEXT NULL,
	    Type TEXT NULL,
	    Size INTEGER NULL,
	    Container TEXT NULL,
	    RetailPrice REAL NULL,
	    Comment TEXT NULL
	)

	CREATE TABLE partsupp(
	    PartKey INTEGER NULL,
	    SuppKey INTEGER NULL,
	    AvailQty INTEGER NULL,
	    SupplyCost REAL NULL,
	    Comment varchar(200) NULL
	)

	CREATE TABLE region(
	    RegionKey INTEGER NULL,
	    Name TEXT NULL,
	    Comment varchar(160) NULL
	)

	CREATE TABLE supplier(
	    SuppKey INTEGER NULL,
	    Name TEXT NULL,
	    Address TEXT NULL,
	    NationKey INTEGER NULL,
	    Phone TEXT NULL,
	    AcctBal REAL NULL,
	    Comment TEXT NULL
	)

)