-- Fixture for test_postgres_to_parquet.py; applied by `make pg-test`.
drop table if exists it_primitives;

create table it_primitives (
    b bool, i2 int2, i4 int4, i8 int8,
    f4 float4, f8 float8, t text, bin bytea
);

insert into it_primitives values
    (true, 1, 2, 3, 1.5, 2.5, 'one', '\x0102'),
    (false, -1, -2, -3, -1.5, -2.5, '', '\x'),
    (null, null, null, null, null, null, null, null);
