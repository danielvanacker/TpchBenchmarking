import duckdb
import helper
from datetime import date

con = duckdb.connect(":memory:")
c = con.cursor()

def main():
    testCon()
    createTables()
    importData()
    sqlLoop()

def testCon():
    c.execute("CREATE TABLE people(name VARCHAR, age INTEGER, sex CHAR(1))")
    c.execute("INSERT INTO people VALUES ('Daniel', 20, 'M')")
    c.execute("SELECT * FROM people")
    print(c.fetchall())

def createTables():
    regionTable = "CREATE TABLE region(r_regionkey TINYINT PRIMARY KEY, r_name CHAR(25), r_comment VARCHAR(152))"

    nationTable = "CREATE TABLE nation(n_nationkey TINYINT PRIMARY KEY, n_name CHAR(25), n_regionkey TINYINT, n_comment VARCHAR(152))"

    partTable = "CREATE TABLE part(p_partkey INT PRIMARY KEY, p_name VARCHAR(55), p_mfgr CHAR(25), p_brand CHAR(10), p_type VARCHAR(25), p_size INT, p_container CHAR(10), p_retailprice REAL, p_comment VARCHAR(23))"

    supplierTable = "CREATE TABLE supplier(s_suppkey INT PRIMARY KEY, s_name CHAR(25), s_address VARCHAR(40), s_nationkey TINYINT, s_phone CHAR(15), s_acctbal REAL, s_comment VARCHAR(101))"

    partsuppTable = "CREATE TABLE partsupp(ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost REAL, ps_comment VARCHAR(199), PRIMARY KEY(ps_partkey, ps_suppkey))"

    customerTable = "CREATE TABLE customer(c_custkey INT PRIMARY KEY, c_name VARCHAR(25), c_address VARCHAR(40), c_nationkey TINYINT, c_phone CHAR(15), c_acctbal REAL, c_mktsegment CHAR(10), c_comment VARCHAR(117))"

    ordersTable = "CREATE TABLE orders(o_orderkey INT PRIMARY KEY, o_custkey INT, o_orderstatus CHAR(1), o_totalprice REAL, o_orderdate DATE, o_orderpriority CHAR(15), o_clerk CHAR(15), o_shippriority INT, o_comment VARCHAR(79))"

    lineitemTable = "CREATE TABLE lineitem(l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity REAL, l_extendedprice REAL, l_discount REAL, l_tax REAL, l_returnflag CHAR(1), l_linestatus CHAR(1), l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHAR(25), l_shipmode CHAR(10), l_comment VARCHAR(44), PRIMARY KEY(l_orderkey, l_linenumber))"

    tables = [regionTable, nationTable, partTable, supplierTable, partsuppTable, customerTable, ordersTable, lineitemTable]
    for table in tables:
        print("Executing:", table)
        c.execute(table)
        print("Complete")

def importData():

    tables = ["region", "nation", "part", "supplier", "partsupp", "customer", "orders", "lineitem"]
    for table in tables:
        toExecute = "COPY " + table +  " FROM '../data/" + table + ".tbl' (DELIMITER '|');"
        print("Executing:", toExecute)
        c.execute(toExecute)
        print("Complete")

def sqlLoop():
    while True:
        command = input("Please enter a SQL statement or exit to quit")
        if(command == "exit"):
            return
        c.execute(command)
        print(c.fetchall())

def query1():
    delta = helper.rand(60, 120)
    select = "l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * ( 1 - l_discount) * ( 1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order"
    fromTbl = "lineitem"
    where = f"l_shipdate <= DATE '1998-12-01' - {str(delta)}"
    group = "l_returnflag, l_linestatus"
    order = "l_returnflag, l_linestatus"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} GROUP BY {group} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())

def query2():
    region = helper.getRName()
    randType = helper.getType()
    size = helper.rand(1, 50)
    subQuery = f"SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '{region}'"
    select = "s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment"
    fromTbl = "part, supplier, partsupp, nation, region"
    where = f"p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = {str(size)} AND p_type like '%{randType}' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '{region}' AND ps_supplycost = ({subQuery})"
    order = "s_acctbal desc, n_name, s_name, p_partkey"
    limit = " FETCH FIRST 100 ROWS ONLY"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} ORDER BY {order} {limit}"

    c.execute(query)
    print(c.fetchall())

def query3():
    segment = helper.getSegment()
    randDate = helper.getRandDate(date(1995, 3, 1), date(1995, 3, 31))
    select = "l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority"
    fromTbl = "customer, orders, lineitem"
    where = f"c_mktsegment = '{segment}' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < date '{randDate}' AND l_shipdate > date '{randDate}'"
    group = "l_orderkey, o_orderdate, o_shippriority"
    order = "revenue desc, o_orderdate"
    limit = "FETCH FIRST 10 ROWS ONLY"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} GROUP BY {group} ORDER BY {order} {limit}"

    c.execute(query)
    print(c.fetchall())

def query4():
    randDate = helper.getRandMonth(date(1993, 1, 1), date(1997, 10, 1))
    addDays = str(helper.monthsToDays(randDate, 3))
    subQuery = f"SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate"
    select = "o_orderpriority, count(*) as order_count"
    fromTbl = "orders"
    where = f"o_orderdate >= date '{randDate}' AND o_orderdate < date '{randDate}' + {addDays} AND exists ({subQuery})"
    group = "o_orderpriority"
    order = "o_orderpriority"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} GROUP BY {group} ORDER BY {order}"
    c.execute(query)
    print(c.fetchall())

if __name__ == "__main__":
    main()
