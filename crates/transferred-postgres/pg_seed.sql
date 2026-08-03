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

drop table if exists it_temporal;

create table it_temporal (d date, ts timestamp, tstz timestamptz);

-- tstz literals carry +00 so they don't depend on session TimeZone.
insert into it_temporal values
    ('2024-01-15', '2024-01-15 12:34:56.789012', '2024-01-15 12:34:56.789012+00'),
    ('1969-07-20', '1969-07-20 20:17:40', '1969-07-20 20:17:40+00'),
    (null, null, null);
