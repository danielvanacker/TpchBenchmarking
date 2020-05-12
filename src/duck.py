import duckdb
import helper
import random
from datetime import date

con = duckdb.connect(":memory:")
c = con.cursor()
sf = 0.1

def main():
    testCon()
    createTables()
    importData()
    query22()
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
        if(command == "18"):
            query18()
            continue
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
    randType = helper.getType3()
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

def query5():
    region = helper.getRName()
    randDate = date(helper.rand(1993, 1997), 1, 1)
    addDays = str(helper.yearsToDays(randDate, 1))
    select = "n_name, sum(l_extendedprice * (1 - l_discount)) as revenue"
    fromTbl = "customer, orders, lineitem, supplier, nation, region"
    where = f"c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '{region}' AND o_orderdate >= date '{randDate}' AND o_orderdate < date '{randDate}' + {addDays}"
    group = "n_name"
    order = "revenue desc"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} GROUP BY {group} ORDER BY {order}"
    
    c.execute(query)
    print(c.fetchall())

def query6():
    randDate = date(helper.rand(1993, 1997), 1, 1)
    discount = str(random.uniform(0.02, 0.09))
    quantity = str(helper.rand(24, 25))
    addDays = addDays = str(helper.yearsToDays(randDate, 1))
    select = "sum(l_extendedprice * l_discount) as revenue"
    fromTbl = "lineitem"
    where = f"l_shipdate >= date '{randDate}' AND l_shipdate < date '{randDate}' + {addDays} AND l_discount between {discount} - 0.01 AND {discount} + 0.01 AND l_quantity < {quantity}"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where}"

    c.execute(query)
    print(c.fetchall())

def query7():
    (nation1, nation2) = helper.getNNames()
    select = "supp_nation, cust_nation, l_year, sum(volume) as revenue"
    subSelect = "n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume"
    subFromTbl = "supplier, lineitem, orders, customer, nation n1, nation n2"
    subWhere = f"s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND ((n1.n_name = '{nation1}' AND n2.n_name = '{nation2}') OR (n1.n_name = '{nation2}' and n2.n_name = '{nation1}')) AND l_shipdate between date '1995-01-01' and date '1996-12-31'"
    group = "supp_nation, cust_nation, l_year"
    order = "supp_nation, cust_nation, l_year"
    query = f"SELECT {select} FROM(SELECT {subSelect} FROM {subFromTbl} WHERE {subWhere}) AS shipping GROUP BY {group} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())

def query8():
    (nation, region) = helper.getNationAndRegion()
    typeString = helper.getTypeString()
    print(typeString)
    select = f"o_year, SUM(CASE WHEN nation = '{nation}' THEN volume ELSE 0 END) / SUM(volume) as mkt_share"
    subSelect = "extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation"
    subFrom = "part, supplier, lineitem, orders, customer, nation n1, nation n2, region"
    subWhere = f"p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = '{region}' AND s_nationkey = n2.n_nationkey AND o_orderdate between date '1995-01-01' and date '1996-12-31' AND p_type = '{typeString}'"
    group = "o_year"
    order = "o_year"
    subQuery = f"SELECT {subSelect} FROM {subFrom} WHERE {subWhere}"
    query = f"SELECT {select} FROM ({subQuery}) as all_nations GROUP BY {group} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())

def query9():
    color = helper.getColor()
    select = "nation, o_year, SUM(amount) as sum_profit"
    subSelect = "n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount"
    subFrom = "part, supplier, lineitem, partsupp, orders, nation"
    subWhere = f"s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name like '%{color}%'"
    group = "nation, o_year"
    order = "nation, o_year DESC"
    subQuery = f"SELECT {subSelect} FROM {subFrom} WHERE {subWhere}"
    query = f"SELECT {select} FROM({subQuery})AS profit GROUP BY {group} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())

def query10():
    randDate = helper.getRandMonth(date(1993, 2, 1), date(1995, 1, 1))
    addDays = str(helper.monthsToDays(randDate, 3))
    select = "c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment"
    fromTbl = "customer, orders, lineitem, nation"
    where = f"c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= date '{randDate}' AND o_orderdate < date '{randDate}' + {addDays} AND l_returnflag = 'R' AND c_nationkey = n_nationkey"
    group = "c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment"
    order = "revenue desc"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} GROUP BY {group} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())

def query11():
    (nation, tmp) = helper.getNNames()
    fraction = 0.0001/sf
    select = "ps_partkey, SUM(ps_supplycost * ps_availqty) as value"
    fromTbl = "partsupp, supplier, nation"
    where = f"ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = '{nation}'"
    subSelect = f"sum(ps_supplycost * ps_availqty) * {fraction}"
    subFromTbl = "partsupp, supplier, nation"
    subWhere = f"ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = '{nation}'"
    group = f"ps_partkey HAVING SUM(ps_supplycost * ps_availqty) > (SELECT {subSelect} FROM {subFromTbl} WHERE {subWhere})"
    order = "value DESC"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} GROUP BY {group} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())

def query12():
    (shipmode1, shipmode2) = helper.getModes()
    randDate = date(helper.rand(1993, 1997), 1, 1)
    addDays = helper.yearsToDays(randDate, 1)
    select = "l_shipmode, SUM(CASE WHEN o_orderpriority ='1-URGENT' OR o_orderpriority ='2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count"
    fromTbl = "orders, lineitem"
    where = f"o_orderkey = l_orderkey AND l_shipmode in ('{shipmode1}', '{shipmode2}') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= date '{randDate}' AND l_receiptdate < date '{randDate}' + {addDays}"
    group = "l_shipmode"
    order = "l_shipmode"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} GROUP BY {group} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())

def query13():
    (word1, word2) = helper.getWords()
    select = "c_count, count(*) as custdist"
    fromTbl = f"(SELECT c_custkey, count(o_orderkey) FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment not like '%{word1}%{word2}%' GROUP BY c_custkey) AS c_orders (c_custkey, c_count)"
    group = "c_count"
    order = "custdist DESC, c_count DESC"
    query = f"SELECT {select} FROM {fromTbl} GROUP BY {group} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())

def query14():
    randDate = date(helper.rand(1993, 1997), helper.rand(1, 12), 1)
    addDays = helper.monthsToDays(randDate, 1)
    select = "100.00 * SUM(CASE WHEN p_type like 'PROMO%' THEN l_extendedprice*(1-l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue"
    fromTbl = "lineitem, part"
    where = f"l_partkey = p_partkey AND l_shipdate >= date '{randDate}' AND l_shipdate < date '{randDate}' + {addDays}"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where}"

    c.execute(query)
    print(c.fetchall())

def query15():
   randDate = helper.getRandMonth(date(1993, 1, 1), date(1997, 10, 1))
   addDays = helper.monthsToDays(randDate, 3)
   view = f"CREATE VIEW revenue (supplier_no, total_revenue) AS SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount)) FROM lineitem WHERE l_shipdate >= date '{randDate}' AND l_shipdate < date '{randDate}' + {addDays} GROUP BY l_suppkey"
   select = "s_suppkey, s_name, s_address, s_phone, total_revenue"
   fromTbl = "supplier, revenue"
   where = "s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM revenue)"
   order = "s_suppkey"
   drop = "DROP VIEW revenue"
   query = f"SELECT {select} FROM {fromTbl} WHERE {where} ORDER BY {order}"

   c.execute(view)
   c.execute(query)
   print(c.fetchall())
   c.execute(drop)

def query16():
    brand = helper.getBrand()
    type = helper.getTypeString2()
    size = str(helper.rand(1, 50))
    for i in range(2, 9):
        size = size + ", " + str(helper.rand(1, 50))
    select = "p_brand, p_type, p_size, COUNT(distinct ps_suppkey) AS supplier_cnt"
    fromTbl = "partsupp, part"
    subQuery = "SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%'"
    where = f"p_partkey = ps_partkey AND p_brand <> '[BRAND]' AND p_type not like '{type}%' AND p_size in ({size}) AND ps_suppkey not in ({subQuery})"
    group = "p_brand, p_type, p_size"
    order = "supplier_cnt DESC, p_brand, p_type, p_size"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} GROUP BY {group} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())
    
def query17():
    brand = helper.getBrand()
    container = helper.getContainer()
    select = "SUM(l_extendedprice) / 7.0 AS avg_yearly"
    fromTbl = "lineitem, part"
    subQuery = "SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey"
    where = f"p_partkey = l_partkey AND p_brand = '{brand}' AND p_container = '{container}' AND l_quantity < ({subQuery})"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where}"

    c.execute(query)
    print(c.fetchall())

def query18():
    quantity = helper.rand(312, 315) #Does not exists for scale factor of 0.1
    select = "c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity)"
    fromTbl = "customer, orders, lineitem"
    subQuery = f"SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > {quantity}"
    where = f"o_orderkey in ({subQuery}) AND c_custkey = o_custkey AND o_orderkey = l_orderkey"
    group = "c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice"
    order = "o_totalprice desc, o_orderdate"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} GROUP BY {group} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())

def query19():
    quantity1 = helper.rand(1, 10)
    quantity2 = helper.rand(10, 20)
    quantity3 = helper.rand(20, 30)
    brand1 = helper.getBrand()
    brand2 = helper.getBrand()
    brand3 = helper.getBrand()
    select = "SUM(l_extendedprice * (1 - l_discount) ) AS revenue"
    fromTbl = "lineitem, part"
    where1 = f"p_partkey = l_partkey AND p_brand = '{brand1}' AND p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= {quantity1} AND l_quantity <= {quantity1} + 10 AND p_size between 1 and 5 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'"
    where2 = f"p_partkey = l_partkey AND p_brand = '{brand2}' AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= {quantity2} and l_quantity <= {quantity2} + 10 AND p_size between 1 and 10 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'"
    where3 = f"p_partkey = l_partkey AND p_brand = '{brand3}' AND p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= {quantity3} AND l_quantity <= {quantity3} + 10 AND p_size between 1 and 15 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'"
    query = f"SELECT {select} FROM {fromTbl} WHERE ({where1}) OR ({where2}) OR ({where3})"

    c.execute(query)
    print(c.fetchall())

def query20():
    color = helper.getColor()
    randDate = date(helper.rand(1993, 1997), 1, 1)
    (nation, tmp) = helper.getNNames()
    addDays = helper.yearsToDays(randDate, 1)
    select = "s_name, s_address"
    fromTbl = "supplier, nation"
    subQuery = f"SELECT ps_suppkey FROM partsupp WHERE ps_partkey in (SELECT p_partkey FROM part WHERE p_name LIKE '{color}%') AND  ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= date '{randDate}' AND l_shipdate < date '{randDate}' + {addDays})"
    where = f"s_suppkey IN ({subQuery}) AND s_nationkey = n_nationkey AND n_name = '{nation}'"
    order = "s_name"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())

def query21():
    (nation, tmp) = helper.getNNames()
    select = "s_name, count(*) as numwait"
    fromTbl = "supplier, lineitem l1, orders, nation"
    where = f"s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND exists (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND not exists (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = '{nation}'"
    group = "s_name"
    order = "numwait DESC, s_name"
    limit = "FETCH FIRST 100 ROWS ONLY"
    query = f"SELECT {select} FROM {fromTbl} WHERE {where} GROUP BY {group} ORDER BY {order} {limit}"

    c.execute(query)
    print(c.fetchall())

def query22():
    codes = helper.getCountryCodes()
    select = "cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal"
    subSelect = "substring(c_phone from 1 for 2) as cntrycode, c_acctbal"
    subFromTbl = "customer"
    subWhere = f"substring(c_phone from 1 for 2) IN ({codes}) AND c_acctbal > (SELECT avg(c_acctbal) FROM customer WHERE c_acctbal > 0.00 AND substring (c_phone from 1 for 2) IN ({codes})) AND not exists (SELECT * FROM orders WHERE o_custkey = c_custkey)"
    subQuery = f"SELECT {subSelect} FROM {subFromTbl} WHERE {subWhere}"
    fromTbl = f"({subQuery}) AS custsale"
    group = "cntrycode"
    order = "cntrycode"
    query = f"SELECT {select} FROM {fromTbl} GROUP BY {group} ORDER BY {order}"

    c.execute(query)
    print(c.fetchall())

if __name__ == "__main__":
    main()
