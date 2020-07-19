# TpchBenchmarking
Supports [monetdblite](https://www.monetdb.org/blog/monetdblite-for-python), [monetdbe](https://pypi.org/project/monetdbe/), [duckdb](https://duckdb.org/), [sqlite](https://www.sqlite.org/index.html), [pandasql](https://pypi.org/project/pandasql/), and [pandas](https://pandas.pydata.org/) [TPC-H bencmarking](http://www.tpc.org/tpch/).

## mainTester.py
Runs tests on monetdb, duckdb, sqlite, and pandasql.
```bash
python mainTester.py <test_target_name> <iterations> <output_csv> <query>
```
Test target options are monet, duck, sqlite, and pandasql.

## monetdbe.py
Runs tests on monetdbe
```bash
python monetdbe.py monetdbe <iterations> <output_csv> <query>
```

## Pandas
Run tests on pandas.
```bash
cd realPanda
python runTPCH-pandas.py <query_number> <iterations> <output_file_name>

