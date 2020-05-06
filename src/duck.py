import duckdb
con = duckdb.connect(":memory:")
c = con.cursor()
c.execute("CREATE TABLE people(name VARCHAR, age INTEGER, sex CHAR(1))")
c.execute("INSERT INTO people VALUES ('Daniel', 20, 'M')")
c.execute("SELECT * FROM people")
print(c.fetchall())

regionTable = "CREATE TABLE region(r_regionkey TINYINT PRIMARY KEY, r_name CHAR(25), r_comment VARCHAR(152))"

nationTable = "CREATE TABLE nation(n_nationkey TINYINT PRIMARY KEY, n_name CHAR(25), n_regionkey TINYINT, n_comment VARCHAR(152))"

partTable = "CREATE TABLE part(p_partkey INT PRIMARY KEY, p_name VARCHAR(55), p_mfgr CHAR(25), p_brand CHAR(10), p_type VARCHAR(25), p_size INT, p_container CHAR(10), p_retailprice REAL, p_comment VARCHAR(23))"

supplierTable = "CREATE TABLE supplier(s_suppkey INT PRIMARY KEY, s_name CHAR(25), s_address VARCHAR(40), s_nationkey TINYINT, s_phone CHAR(15), s_acctbal REAL, s_comment VARCHAR(101))"

partsuppTable = "CREATE TABLE partsupp(ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost REAL, ps_comment VARCHAR(199), PRIMARY KEY(ps_partkey, ps_suppkey))"

customerTable = "CREATE TABLE customer(c_custkey INT PRIMARY KEY, c_name VARCHAR(25), c_address VARCHAR(40), c_nationkey TINYINT, c_phone CHAR(15), c_acctbal REAL, c_mktsegment CHAR(10), c_comment VARCHAR(117))"

ordersTable = "CREATE TABLE orders(o_orderkey INT PRIMARY KEY, o_custkey INT, o_orderstatus CHAR(1), o_totalprice REAL, o_orderdate DATE, o_orderpriority CHAR(15), o_clerk CHAR(15), o_shippriority INT, o_comment VARCHAR(79))"

lineitemTable = "CREATE TABLE lineitem(l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity REAL, l_extendedprice REAL, l_discount REAL, l_tax REAL, l_returnflag CHAR(1), l_linestatus CHAR(1), l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHAR(25), l_shipmode CHAR(10), l_comment VARCHAR(44), PRIMARY KEY(l_orderkey, l_linenumber))"

c.execute(regionTable)
c.execute(nationTable)
c.execute(partTable)
c.execute(supplierTable)
c.execute(partsuppTable)
c.execute(customerTable)
c.execute(ordersTable)
c.execute(lineitemTable)

tables = ["region", "nation", "part", "supplier", "partsupp", "customer", "orders", "lineitem"]
for table in tables:
    toExecute = "COPY " + table +  " FROM '../data/" + table + ".tbl' (DELIMITER '|');"
    print(toExecute)
    c.execute(toExecute)

c.execute("SELECT * FROM region")
print(c.fetchall())


