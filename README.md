# TpchBenchmarking
Supports monetdb, monetdbe, duckdb, sqlite, pandasql, and pandas TPC-H bencmarking.

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

