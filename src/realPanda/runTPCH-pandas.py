import pandas as pd
import pymonetdb
import sys
import pandas.io.sql as psql
import os
from time import time
import monetdblite

config = __import__('TPCHconfig-pandas')
tpchqueries = __import__('TPCHqueries-pandas')

assert len(sys.argv) > 1, 'Usage: python3 runTPCH-pandas.py <query_number>, <iterations>, <output_file_name>'
query = int(sys.argv[1])
numRuns = int(sys.argv[2])
outputFileName = str(sys.argv[3])


class Database:

    def __init__(self):
        self.con = monetdblite.make_connection("tempDb")

    def setBufferSize(self, size):
        self.con.replysize = size

    def __getattr__(self, name):
        t = pd.DataFrame(psql.read_sql_query(f'SELECT * FROM {name};', self.con))
        for (columnName, columnData) in t.iteritems():
            if("date" in columnName):
                t[columnName] = pd.to_datetime(t[columnName])
        self.__setattr__(name, t)
        return t

    def createTables(self):
        regionTable = "CREATE TABLE region(r_regionkey TINYINT PRIMARY KEY, r_name CHAR(25), r_comment VARCHAR(152))"
        nationTable = "CREATE TABLE nation(n_nationkey TINYINT PRIMARY KEY, n_name CHAR(25), n_regionkey TINYINT, n_comment VARCHAR(152))"
        partTable = "CREATE TABLE part(p_partkey INT PRIMARY KEY, p_name VARCHAR(55), p_mfgr CHAR(25), p_brand CHAR(10), p_type VARCHAR(25), p_size INT, p_container CHAR(10), p_retailprice REAL, p_comment VARCHAR(23))"
        supplierTable = "CREATE TABLE supplier(s_suppkey INT PRIMARY KEY, s_name CHAR(25), s_address VARCHAR(40), s_nationkey TINYINT, s_phone CHAR(15), s_acctbal REAL, s_comment VARCHAR(101))"
        partsuppTable = "CREATE TABLE partsupp(ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost REAL, ps_comment VARCHAR(199), PRIMARY KEY(ps_partkey, ps_suppkey))"
        customerTable = "CREATE TABLE customer(c_custkey INT PRIMARY KEY, c_name VARCHAR(25), c_address VARCHAR(40), c_nationkey TINYINT, c_phone CHAR(15), c_acctbal REAL, c_mktsegment CHAR(10), c_comment VARCHAR(117))"
        ordersTable = "CREATE TABLE orders(o_orderkey INT PRIMARY KEY, o_custkey INT, o_orderstatus CHAR(1), o_totalprice REAL, o_orderdate DATE, o_orderpriority CHAR(15), o_clerk CHAR(15), o_shippriority INT, o_comment VARCHAR(79))"
        lineitemTable = "CREATE TABLE lineitem(l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity REAL, l_extendedprice REAL, l_discount REAL, l_tax REAL, l_returnflag CHAR(1), l_linestatus CHAR(1), l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHAR(25), l_shipmode CHAR(10), l_comment VARCHAR(44), PRIMARY KEY(l_orderkey, l_linenumber))"

        tables = [regionTable, nationTable, partTable, supplierTable, partsuppTable, customerTable, ordersTable, lineitemTable]
        cursor = self.con.cursor()
        for table in tables:
            print("Executing:", table)
            cursor.execute(table)
            print("Complete")

    def importData(self):
        cursor = self.con.cursor()
        tables = ["region", "nation", "part", "supplier", "partsupp", "customer", "orders", "lineitem"]
        for table in tables:
            toExecute = "COPY INTO " + table +  " FROM '/scratch/dvanac/data_4/" + table + ".tbl' USING DELIMITERS '|', '\n';"
            print("Executing:", toExecute)
            cursor.execute(toExecute)
            print("Complete")

db = Database()
db.setBufferSize(config.databuffersize)
db.createTables()
db.importData()

#os.system('mkdir -p {}'.format(config.outputDir))

if config.preload:
    print('----------[ Preloading ]----------')
    t0 = time()
    for t in ('lineitem', 'part', 'partsupp', 'customer', 'orders', 'nation', 'region', 'supplier'):
        getattr(db, t)
    t1 = time()
    print('Preloading all tables took {}s'.format(t1 - t0))

with open('{0}/{1}.csv'.format(config.outputDir, outputFileName), 'a') as f:
    for x in range(0, int(numRuns)):
        print('----------[ Query {0} ]----------'.format(query))
        (r, totTime) = getattr(tpchqueries, 'q' + str(query))(db)
        print(r)
        print('Executing query took {}s'.format(totTime))
        f.write('{0},{1}\n'.format(int(query), totTime))
