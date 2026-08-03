-- Fixture for the `pg_to_arrow` integration test; applied on container boot.
-- Tested at crates/transferred-postgres/tests/pg_to_arrow.rs
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

create table it_temporal (d date, ts timestamp, tstz timestamptz, iv interval);

-- tstz literals carry +00 so they don't depend on session TimeZone.
-- iv keeps months/days/micros mutually irreducible, as PG never normalises across them.
insert into it_temporal values
    ('2024-01-15', '2024-01-15 12:34:56.789012', '2024-01-15 12:34:56.789012+00',
     '1 year 2 mons 3 days 04:05:06.789'),
    ('1969-07-20', '1969-07-20 20:17:40', '1969-07-20 20:17:40+00',
     '-1 mons -2 days -03:00:00'),
    (null, null, null, null);

drop table if exists it_numeric;

-- `n` is bare on purpose: it exercises the Decimal128(38, 9) default for typmod -1.
create table it_numeric (n numeric, small numeric(28,4), wide numeric(38,9));

-- Each row holds one value in all three columns, so the asserted units differ only by scale.
-- Row 2 is 28 significant digits: rust_decimal's 96-bit mantissa stops short of Decimal128's 38.
-- Row 3 is a rounding midpoint. PG rounds it on write for the declared columns, but bare `n` keeps
-- all 10 decimals, so only there does the mapping itself round — half away from zero.
insert into it_numeric values
    (1.5, 1.5, 1.5),
    (-1234567890123456789.123456789, -1234567890123456789.123456789, -1234567890123456789.123456789),
    (0.1234567885, 0.1234567885, 0.1234567885),
    (null, null, null);

drop table if exists it_semantic;

create table it_semantic (u uuid, j json, jb jsonb);

insert into it_semantic values
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a": [1]}', '{"a": [1]}'),
    ('00000000-0000-0000-0000-000000000000', '[]', '[]'),
    (null, null, null);
