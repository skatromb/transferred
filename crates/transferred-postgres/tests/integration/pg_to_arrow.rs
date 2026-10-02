//! PG → Arrow mapping against a throwaway Postgres container seeded by `pg_seed.sql`. Needs Docker.

use std::str;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, new_null_array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::concat;
use arrow::datatypes::IntervalMonthDayNano;
use arrow_schema::extension::{ExtensionType, Json, Opaque, Uuid};
use arrow_schema::{DataType, Field, Schema};
use geoarrow_schema::WkbType;
use transferred_postgres::{PgRange, geoarrow};

use crate::common::read_table;

/// 2024-01-15, in days since the Unix epoch.
const JAN_15_2024: i32 = 19_737;
/// 2024-01-21, in days since the Unix epoch.
const JAN_21_2024: i32 = 19_743;
/// 1969-07-20, in days since the Unix epoch. Before it, and before PG's own 2000-01-01 epoch, so
/// the count is negative on both sides of the shift between them.
const JUL_20_1969: i32 = -165;
/// 2024-01-15 12:34:56.789012, in microseconds since the Unix epoch.
const JAN_15_2024_12_34_56: i64 = 1_705_322_096_789_012;
/// 2024-01-16 00:00:00, in microseconds since the Unix epoch.
const JAN_16_2024: i64 = 1_705_363_200_000_000;
/// 1969-07-20 20:17:40, in microseconds since the Unix epoch; negative for the same reason.
const JUL_20_1969_20_17_40: i64 = -14_182_940_000_000;

/// The zone every `timestamptz` arrives in.
const UTC: &str = "UTC";
/// What a bare `numeric` maps to, and so does `numeric(38,9)`.
const BARE_NUMERIC: DataType = DataType::Decimal128(38, 9);
/// `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`, the example UUID of the PG docs.
const PG_DOCS_UUID: u128 = 0xa0ee_bc99_9c0b_4ef8_bb6d_6bb9_bd38_0a11;
/// Width of a UUID, as Arrow's `FixedSizeBinary` counts it.
const UUID_BYTES: i32 = 16;

/// `SRID=4326;POINT(1 2)` as `geometry_send` writes it. `0x20` in the type word flags a trailing
/// SRID, here `e6100000`.
const POINT_4326: &str = "0101000020e6100000000000000000f03f0000000000000040";
/// `SRID=3006;LINESTRING(0 0, 1 1)`, its SRID `be0b0000`.
const LINE_3006: &str = "0102000020be0b0000020000000000000000000000000000000000\
                         0000000000000000f03f000000000000f03f";
/// `SRID=4326;POINT(-73.985 40.748)`.
const CITY_4326: &str = "0101000020e6100000d7a3703d0a7f52c039b4c876be5f4440";
/// `SRID=4269;POINT(1 2)`.
const POINT_4269: &str = "0101000020ad100000000000000000f03f0000000000000040";
/// `POINT(1 2)` with no SRID at all, so no flag in the type word either.
const POINT_NO_SRID: &str = "0101000000000000000000f03f0000000000000040";
/// `POINT(3 4)` with no SRID.
const OTHER_POINT_NO_SRID: &str = "010100000000000000000008400000000000001040";

/// Assembles a batch from its columns, each a field and its values.
fn batch(columns: impl IntoIterator<Item = (Field, ArrayRef)>) -> RecordBatch {
    let (fields, arrays): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("fields match columns")
}

/// A nullable column named `name`, typed by its own values.
fn column(name: &str, values: impl Array + 'static) -> (Field, ArrayRef) {
    let field = Field::new(name, values.data_type().clone(), true);
    (field, Arc::new(values))
}

/// A column whose field carries `extension`, typed by its own values.
fn tagged(
    name: &str,
    extension: impl ExtensionType,
    values: impl Array + 'static,
) -> (Field, ArrayRef) {
    let (field, array) = column(name, values);
    (field.with_extension_type(extension), array)
}

/// The columns of `table` that `expected` has, in its order, so a test can check a few at a time.
async fn read_columns(table: &str, expected: &RecordBatch) -> RecordBatch {
    let whole = read_table(table).await;
    let indices: Vec<_> = expected
        .schema()
        .fields()
        .iter()
        .map(|field| {
            whole
                .schema()
                .index_of(field.name())
                .expect("column exists")
        })
        .collect();
    whole.project(&indices).expect("indices in range")
}

#[tokio::test]
async fn primitives() {
    let expected = batch([
        column("b", BooleanArray::from(vec![Some(true), Some(false), None])),
        column("i2", Int16Array::from(vec![Some(1), Some(-1), None])),
        column("i4", Int32Array::from(vec![Some(2), Some(-2), None])),
        column("i8", Int64Array::from(vec![Some(3), Some(-3), None])),
        column("f4", Float32Array::from(vec![Some(1.5), Some(-1.5), None])),
        column("f8", Float64Array::from(vec![Some(2.5), Some(-2.5), None])),
        column("t", StringArray::from(vec![Some("one"), Some(""), None])),
        column(
            "bin",
            BinaryArray::from(vec![Some(&[1_u8, 2][..]), Some(&[][..]), None]),
        ),
    ]);

    assert_eq!(read_table("it_primitives").await, expected);
}

#[tokio::test]
async fn temporal() {
    let micros = vec![Some(JAN_15_2024_12_34_56), Some(JUL_20_1969_20_17_40), None];

    // Months, days and micros stay separate — PG carries all three independently.
    let intervals = vec![
        Some(IntervalMonthDayNano::new(14, 3, 14_706_789_000_000)),
        Some(IntervalMonthDayNano::new(-1, -2, -10_800_000_000_000)),
        None,
    ];

    let expected = batch([
        column(
            "d",
            Date32Array::from(vec![Some(JAN_15_2024), Some(JUL_20_1969), None]),
        ),
        column("ts", TimestampMicrosecondArray::from(micros.clone())),
        column(
            "tstz",
            TimestampMicrosecondArray::from(micros).with_timezone(UTC),
        ),
        column("iv", IntervalMonthDayNanoArray::from(intervals)),
    ]);

    assert_eq!(read_table("it_temporal").await, expected);
}

#[tokio::test]
async fn numeric() {
    // Arrow stores decimals as integer counts of 10^-scale units.
    // Row 3 arrives at scale 10, so bare `n` is the one column the mapping rounds itself.
    let at_scale_9 = Decimal128Array::from(vec![
        Some(1_500_000_000),
        Some(-1_234_567_890_123_456_789_123_456_789),
        Some(123_456_789),
        None,
    ])
    .with_data_type(BARE_NUMERIC);
    let at_scale_4 = Decimal128Array::from(vec![
        Some(15_000),
        Some(-12_345_678_901_234_567_891_235),
        Some(1_235),
        None,
    ])
    .with_data_type(DataType::Decimal128(28, 4));

    let expected = batch([
        column("n", at_scale_9.clone()),
        column("small", at_scale_4),
        column("wide", at_scale_9),
    ]);

    assert_eq!(read_table("it_numeric").await, expected);
}

#[tokio::test]
async fn semantic() {
    let uuids = [Some(PG_DOCS_UUID), Some(0), None].map(|uuid| uuid.map(u128::to_be_bytes));
    let docs = vec![Some(r#"{"a": [1]}"#), Some("[]"), None];

    let expected = batch([
        tagged(
            "u",
            Uuid,
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(uuids.into_iter(), UUID_BYTES)
                .unwrap(),
        ),
        tagged("j", Json::default(), StringArray::from(docs.clone())),
        tagged("jb", Json::default(), StringArray::from(docs)),
    ]);

    assert_eq!(read_table("it_semantic").await, expected);
}

/// A type PG sends as text arrives as text, whatever OID it was given.
#[tokio::test]
async fn text_extensions() {
    let expected = batch([
        column(
            "mood",
            StringArray::from(vec![Some("glad"), Some("sad"), None]),
        ),
        column(
            "email",
            StringArray::from(vec![Some("Foo@Example.COM"), Some(""), None]),
        ),
    ]);

    assert_eq!(read_table("it_text").await, expected);
}

/// Builds one range column of the fixture, tagged `transferred.pg_range` and bounded by `lower`'s
/// type. The bounds and brackets, as PG prints them, cover the two rows that differ per column;
/// the two that read the same in every column follow — row 3 is `empty`, row 4 is the SQL NULL.
fn ranges(
    name: &str,
    lower: impl Array,
    upper: impl Array,
    brackets: [&str; 2],
) -> (Field, ArrayRef) {
    let fields = PgRange::fields(lower.data_type().clone());
    let unbounded = new_null_array(lower.data_type(), 2);
    let pad = |bounds: &dyn Array| concat(&[bounds, unbounded.as_ref()]).unwrap();
    let lower_inc = brackets.map(|pair| pair.starts_with('['));
    let upper_inc = brackets.map(|pair| pair.ends_with(']'));
    let columns: Vec<ArrayRef> = vec![
        pad(&lower),
        pad(&upper),
        Arc::new(BooleanArray::from([lower_inc, [false; 2]].concat())),
        Arc::new(BooleanArray::from([upper_inc, [false; 2]].concat())),
        Arc::new(BooleanArray::from(vec![false, false, true, false])),
    ];
    let nulls = NullBuffer::from(vec![true, true, true, false]);

    let array = StructArray::try_new(fields, columns, Some(nulls)).unwrap();
    tagged(name, PgRange, array)
}

/// Bounds plus a tag tell an infinite bound, an `empty` range and a SQL NULL apart. A discrete
/// range reaches us canonicalised, so its flags say `[)` whatever the literal wrote.
#[tokio::test]
async fn discrete_ranges_arrive_canonicalised() {
    let expected = batch([
        ranges(
            "i4",
            Int32Array::from(vec![Some(1), None]),
            Int32Array::from(vec![6, 7]),
            ["[)", "()"],
        ),
        ranges(
            "i8",
            Int64Array::from(vec![1, 7]),
            Int64Array::from(vec![Some(6), None]),
            ["[)", "[)"],
        ),
        // The `[)` form of `[2024-01-15,2024-01-20]`.
        ranges(
            "d",
            Date32Array::from(vec![JAN_15_2024; 2]),
            Date32Array::from(vec![Some(JAN_21_2024), None]),
            ["[)", "[)"],
        ),
    ]);

    assert_eq!(read_columns("it_range", &expected).await, expected);
}

/// `numrange`, `tsrange` and `tstzrange` keep the inclusivity of the literal.
#[tokio::test]
async fn continuous_ranges_keep_their_brackets() {
    let lower_micros = TimestampMicrosecondArray::from(vec![Some(JAN_15_2024_12_34_56), None]);
    let upper_micros = TimestampMicrosecondArray::from(vec![JAN_16_2024; 2]);

    let expected = batch([
        ranges(
            "n",
            Decimal128Array::from(vec![Some(1_500_000_000), None]).with_data_type(BARE_NUMERIC),
            Decimal128Array::from(vec![2_500_000_000; 2]).with_data_type(BARE_NUMERIC),
            ["(]", "()"],
        ),
        ranges(
            "ts",
            lower_micros.clone(),
            upper_micros.clone(),
            ["[)", "()"],
        ),
        ranges(
            "tstz",
            lower_micros.with_timezone(UTC),
            upper_micros.with_timezone(UTC),
            ["[)", "()"],
        ),
    ]);

    assert_eq!(read_columns("it_range", &expected).await, expected);
}

/// Decodes a hex byte string, so expectations read as the hex PG itself prints.
fn hex_bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks(2)
        .map(|byte| {
            let byte = str::from_utf8(byte).expect("ascii hex");
            u8::from_str_radix(byte, 16).expect("hex byte")
        })
        .collect()
}

/// A `geoarrow.wkb` column, as the mapping tags a `PostGIS` one, with its values in hex.
fn wkb(name: &str, wkb: WkbType, hex: [Option<&str>; 3]) -> (Field, ArrayRef) {
    let values = BinaryArray::from_iter(hex.map(|cell| cell.map(hex_bytes)));
    tagged(name, wkb, values)
}

/// `PostGIS` sends EWKB, which `geoarrow.wkb` accepts verbatim, so the coordinate
/// system rides along in every value even where the column declares none.
#[tokio::test]
async fn geometry() {
    let expected = batch([
        wkb(
            "geom",
            geoarrow::planar(None),
            [Some(POINT_4326), Some(LINE_3006), None],
        ),
        wkb(
            "pt",
            geoarrow::planar(Some(4326)),
            [Some(POINT_4326), Some(CITY_4326), None],
        ),
        wkb(
            "nosrid",
            geoarrow::planar(None),
            [Some(POINT_NO_SRID), Some(OTHER_POINT_NO_SRID), None],
        ),
    ]);

    assert_eq!(read_columns("it_geo", &expected).await, expected);
}

/// `geography` arrives the same way, tagged spherical so its edges bend around the globe.
#[tokio::test]
async fn geography() {
    let expected = batch([
        wkb(
            "geog",
            geoarrow::spherical(Some(4326)),
            [Some(POINT_4326), Some(CITY_4326), None],
        ),
        // An unconstrained `geography` takes any geographic SRID, 4269 here, not just 4326.
        wkb(
            "bare",
            geoarrow::spherical(None),
            [Some(POINT_4269), Some(POINT_4326), None],
        ),
    ]);

    assert_eq!(read_columns("it_geo", &expected).await, expected);
}

/// A type with no mapping keeps its bytes and says what it was, instead of failing the read.
#[tokio::test]
async fn unmapped_types() {
    let one_two = hex_bytes("00000002000000170000000400000001000000170000000400000002");
    let three_null = hex_bytes("0000000200000017000000040000000300000017ffffffff");

    let expected = batch([
        tagged(
            "mac",
            Opaque::new("macaddr", "PostgreSQL"),
            BinaryArray::from(vec![
                Some(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0x03][..]),
                Some(&[0xff; 6][..]),
                None,
            ]),
        ),
        // PG's record framing: field count, then each field's type OID, byte length and bytes.
        // `int4` is OID 23, and a null field is a length of -1 followed by nothing.
        tagged(
            "point",
            Opaque::new("it_point", "PostgreSQL"),
            BinaryArray::from(vec![
                Some(one_two.as_slice()),
                Some(three_null.as_slice()),
                None,
            ]),
        ),
    ]);

    assert_eq!(read_table("it_opaque").await, expected);
}
