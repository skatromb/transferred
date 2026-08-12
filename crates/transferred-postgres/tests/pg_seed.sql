-- Fixture for the `pg_to_arrow` integration test; applied on container boot.
-- Tested at crates/transferred-postgres/tests/integration/pg_to_arrow.rs
create table it_primitives (
    b bool, i2 int2, i4 int4, i8 int8,
    f4 float4, f8 float8, t text, bin bytea
);

insert into it_primitives values
    (true, 1, 2, 3, 1.5, 2.5, 'one', '\x0102'),
    (false, -1, -2, -3, -1.5, -2.5, '', '\x'),
    (null, null, null, null, null, null, null, null);

create table it_temporal (d date, ts timestamp, tstz timestamptz, iv interval);

-- tstz literals carry +00 so they don't depend on session TimeZone.
-- iv keeps months/days/micros mutually irreducible, as PG never normalises across them.
insert into it_temporal values
    ('2024-01-15', '2024-01-15 12:34:56.789012', '2024-01-15 12:34:56.789012+00',
     '1 year 2 mons 3 days 04:05:06.789'),
    ('1969-07-20', '1969-07-20 20:17:40', '1969-07-20 20:17:40+00',
     '-1 mons -2 days -03:00:00'),
    (null, null, null, null);

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

create table it_semantic (u uuid, j json, jb jsonb);

insert into it_semantic values
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a": [1]}', '{"a": [1]}'),
    ('00000000-0000-0000-0000-000000000000', '[]', '[]'),
    (null, null, null);

create extension if not exists citext;

-- Two types whose wire form already is their text. `citext` has no fixed OID, so goes by name.
create type it_mood as enum ('glad', 'sad');
create table it_text (mood it_mood, email citext);

insert into it_text values
    ('glad', 'Foo@Example.COM'),
    ('sad', ''),
    (null, null);

create extension if not exists postgis;

-- `geom` is bare on purpose: such a column accepts mixed SRIDs, so its coordinate system lives in
-- each value's EWKB rather than in the column type. `nosrid` constrains the subtype but no
-- coordinate system, which PostGIS records as SRID 0. `geog` measures on a sphere, the rest planar.
-- `bare` holds NAD83, not the 4326 an unconstrained `geography` defaults to, so its column cannot
-- claim a coordinate system either.
create table it_geo (
    geom geometry,
    pt geometry(Point, 4326),
    nosrid geometry(Point),
    geog geography(Point, 4326),
    bare geography
);

insert into it_geo values
    ('SRID=4326;POINT(1 2)', 'SRID=4326;POINT(1 2)', 'POINT(1 2)', 'SRID=4326;POINT(1 2)',
     'SRID=4269;POINT(1 2)'),
    ('SRID=3006;LINESTRING(0 0, 1 1)',
     'SRID=4326;POINT(-73.985 40.748)',
     'POINT(3 4)',
     'SRID=4326;POINT(-73.985 40.748)',
     'SRID=4326;POINT(1 2)'),
    (null, null, null, null, null);

-- Two types the mapping has no rule for: one built in, one user-defined, so the type name in the
-- `arrow.opaque` metadata has to come from the catalogue rather than a fixed list.
create type it_point as (x int4, y int4);
create table it_opaque (mac macaddr, point it_point);

-- macaddr goes on the wire as its six bytes; a composite as PG's record framing.
insert into it_opaque values
    ('08:00:2b:01:02:03', '(1,2)'),
    ('ff:ff:ff:ff:ff:ff', '(3,)'),
    (null, null);
