[insert into t1 (id, d32) values (1, "0.0")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (2, "-0.0")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (3, "0e0")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (4, "-0e0")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (5, "1")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (6, "-1")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (7, "1.0")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (8, "10e-1")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (9, "0.1e10")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (10, NULL)] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (11, "12.345e56")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (12, "9999999e89")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (13, "-9999999e89")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (14, "0.000001e-95")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[insert into t1 (id, d32) values (15, "-0.000001e-95")] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[select * from t1 order by d32,id] failed with rc -1 cdb2_run_statement_typed_int: Cannot connect to db
[set transaction snapshot] rc 0
