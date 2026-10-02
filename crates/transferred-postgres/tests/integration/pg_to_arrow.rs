//! PG → Arrow mapping against a throwaway Postgres container seeded by `pg_seed.sql`. Needs Docker.

use std::str;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, new_null_array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::concat;
use arrow::datatypes::IntervalMonthDayNano;
use arrow_schema::extension::{Json, Opaque, Uuid};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
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

/// Assembles a batch from its fields and their columns.
fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("fields match columns")
}

fn nullable(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

#[tokio::test]
async fn primitives() {
    let expected = batch(
        vec![
            nullable("b", DataType::Boolean),
            nullable("i2", DataType::Int16),
            nullable("i4", DataType::Int32),
            nullable("i8", DataType::Int64),
            nullable("f4", DataType::Float32),
            nullable("f8", DataType::Float64),
            nullable("t", DataType::Utf8),
            nullable("bin", DataType::Binary),
        ],
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(Int16Array::from(vec![Some(1), Some(-1), None])),
            Arc::new(Int32Array::from(vec![Some(2), Some(-2), None])),
            Arc::new(Int64Array::from(vec![Some(3), Some(-3), None])),
            Arc::new(Float32Array::from(vec![Some(1.5), Some(-1.5), None])),
            Arc::new(Float64Array::from(vec![Some(2.5), Some(-2.5), None])),
            Arc::new(StringArray::from(vec![Some("one"), Some(""), None])),
            Arc::new(BinaryArray::from(vec![
                Some(&[1_u8, 2][..]),
                Some(&[][..]),
                None,
            ])),
        ],
    );

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

    let expected = batch(
        vec![
            nullable("d", DataType::Date32),
            nullable("ts", DataType::Timestamp(TimeUnit::Microsecond, None)),
            nullable(
                "tstz",
                DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            ),
            nullable("iv", DataType::Interval(IntervalUnit::MonthDayNano)),
        ],
        vec![
            Arc::new(Date32Array::from(vec![
                Some(JAN_15_2024),
                Some(JUL_20_1969),
                None,
            ])),
            Arc::new(TimestampMicrosecondArray::from(micros.clone())),
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone(UTC)),
            Arc::new(IntervalMonthDayNanoArray::from(intervals)),
        ],
    );

    assert_eq!(read_table("it_temporal").await, expected);
}

#[tokio::test]
async fn numeric() {
    // Arrow stores decimals as integer counts of 10^-scale units.
    let small = DataType::Decimal128(28, 4);
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
    .with_data_type(small.clone());

    let expected = batch(
        vec![
            nullable("n", BARE_NUMERIC),
            nullable("small", small),
            nullable("wide", BARE_NUMERIC),
        ],
        vec![
            Arc::new(at_scale_9.clone()),
            Arc::new(at_scale_4),
            Arc::new(at_scale_9),
        ],
    );

    assert_eq!(read_table("it_numeric").await, expected);
}

#[tokio::test]
async fn semantic() {
    let uuids = [Some(PG_DOCS_UUID), Some(0), None].map(|uuid| uuid.map(u128::to_be_bytes));
    let docs = vec![Some(r#"{"a": [1]}"#), Some("[]"), None];

    let expected = batch(
        vec![
            nullable("u", DataType::FixedSizeBinary(UUID_BYTES)).with_extension_type(Uuid),
            nullable("j", DataType::Utf8).with_extension_type(Json::default()),
            nullable("jb", DataType::Utf8).with_extension_type(Json::default()),
        ],
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(uuids.into_iter(), UUID_BYTES)
                    .unwrap(),
            ),
            Arc::new(StringArray::from(docs.clone())),
            Arc::new(StringArray::from(docs)),
        ],
    );

    assert_eq!(read_table("it_semantic").await, expected);
}

/// A type PG sends as text arrives as text, whatever OID it was given.
#[tokio::test]
async fn text_extensions() {
    let expected = batch(
        vec![
            nullable("mood", DataType::Utf8),
            nullable("email", DataType::Utf8),
        ],
        vec![
            Arc::new(StringArray::from(vec![Some("glad"), Some("sad"), None])),
            Arc::new(StringArray::from(vec![
                Some("Foo@Example.COM"),
                Some(""),
                None,
            ])),
        ],
    );

    assert_eq!(read_table("it_text").await, expected);
}

/// Builds one range column of the fixture, tagged `transferred.pg_range` and bounded by `lower`'s
/// type. The bounds and brackets, as PG prints them, cover the two rows that differ per column;
/// the two that read the same in every column follow — row 3 is `empty`, row 4 is the SQL NULL.
fn ranges(name: &str, lower: ArrayRef, upper: ArrayRef, brackets: [&str; 2]) -> (Field, ArrayRef) {
    let fields = PgRange::fields(lower.data_type().clone());
    let field = nullable(name, DataType::Struct(fields.clone())).with_extension_type(PgRange);
    let unbounded = new_null_array(lower.data_type(), 2);
    let pad = |bounds: ArrayRef| concat(&[bounds.as_ref(), unbounded.as_ref()]).unwrap();
    let lower_inc = brackets.map(|pair| pair.starts_with('['));
    let upper_inc = brackets.map(|pair| pair.ends_with(']'));
    let columns: Vec<ArrayRef> = vec![
        pad(lower),
        pad(upper),
        Arc::new(BooleanArray::from([lower_inc, [false; 2]].concat())),
        Arc::new(BooleanArray::from([upper_inc, [false; 2]].concat())),
        Arc::new(BooleanArray::from(vec![false, false, true, false])),
    ];
    let nulls = NullBuffer::from(vec![true, true, true, false]);

    let array = StructArray::try_new(fields, columns, Some(nulls)).unwrap();

    (field, Arc::new(array))
}

/// Bounds plus a tag is the only Arrow shape that tells an infinite bound, an `empty` range and a
/// SQL NULL apart. A discrete range reaches us canonicalised, so its flags say `[)` whatever we
/// wrote; only `numrange`, `tsrange` and `tstzrange` keep the inclusivity of the literal.
#[tokio::test]
async fn ranges_carry_their_bounds_and_tag() {
    let lower_micros = TimestampMicrosecondArray::from(vec![Some(JAN_15_2024_12_34_56), None]);
    let upper_micros = TimestampMicrosecondArray::from(vec![JAN_16_2024; 2]);

    let (fields, columns) = [
        ranges(
            "i4",
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Arc::new(Int32Array::from(vec![6, 7])),
            ["[)", "()"],
        ),
        ranges(
            "i8",
            Arc::new(Int64Array::from(vec![1, 7])),
            Arc::new(Int64Array::from(vec![Some(6), None])),
            ["[)", "[)"],
        ),
        ranges(
            "n",
            Arc::new(
                Decimal128Array::from(vec![Some(1_500_000_000), None]).with_data_type(BARE_NUMERIC),
            ),
            Arc::new(Decimal128Array::from(vec![2_500_000_000; 2]).with_data_type(BARE_NUMERIC)),
            ["(]", "()"],
        ),
        // The `[)` form of `[2024-01-15,2024-01-20]`.
        ranges(
            "d",
            Arc::new(Date32Array::from(vec![JAN_15_2024; 2])),
            Arc::new(Date32Array::from(vec![Some(JAN_21_2024), None])),
            ["[)", "[)"],
        ),
        ranges(
            "ts",
            Arc::new(lower_micros.clone()),
            Arc::new(upper_micros.clone()),
            ["[)", "()"],
        ),
        ranges(
            "tstz",
            Arc::new(lower_micros.with_timezone(UTC)),
            Arc::new(upper_micros.with_timezone(UTC)),
            ["[)", "()"],
        ),
    ]
    .into_iter()
    .unzip();

    assert_eq!(read_table("it_range").await, batch(fields, columns));
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

/// Builds a nullable `geoarrow.wkb` field, as the mapping tags a `PostGIS` column.
fn wkb(name: &str, wkb: WkbType) -> Field {
    nullable(name, DataType::Binary).with_extension_type(wkb)
}

/// `PostGIS` sends EWKB, which `geoarrow.wkb` accepts verbatim, so the coordinate
/// system rides along in every value even where the column declares none.
#[tokio::test]
async fn geometry() {
    // `geometry_send` output byte for byte. `0x20` in the type word flags a trailing SRID:
    // `e6100000` is 4326 and `be0b0000` is 3006; the plain `POINT`s carry no SRID at all.
    let point_4326 = hex_bytes("0101000020e6100000000000000000f03f0000000000000040");
    let line_3006 = hex_bytes(
        "0102000020be0b0000020000000000000000000000000000000000\
         0000000000000000f03f000000000000f03f",
    );
    let city_4326 = hex_bytes("0101000020e6100000d7a3703d0a7f52c039b4c876be5f4440");
    let point = hex_bytes("0101000000000000000000f03f0000000000000040");
    let other_point = hex_bytes("010100000000000000000008400000000000001040");
    let point_4269 = hex_bytes("0101000020ad100000000000000000f03f0000000000000040");

    let expected = batch(
        vec![
            wkb("geom", geoarrow::planar(None)),
            wkb("pt", geoarrow::planar(Some(4326))),
            wkb("nosrid", geoarrow::planar(None)),
            wkb("geog", geoarrow::spherical(Some(4326))),
            // An unconstrained `geography` takes any geographic SRID, 4269 here, not just 4326.
            wkb("bare", geoarrow::spherical(None)),
        ],
        vec![
            Arc::new(BinaryArray::from(vec![
                Some(point_4326.as_slice()),
                Some(line_3006.as_slice()),
                None,
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(point_4326.as_slice()),
                Some(city_4326.as_slice()),
                None,
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(point.as_slice()),
                Some(other_point.as_slice()),
                None,
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(point_4326.as_slice()),
                Some(city_4326.as_slice()),
                None,
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(point_4269.as_slice()),
                Some(point_4326.as_slice()),
                None,
            ])),
        ],
    );

    assert_eq!(read_table("it_geo").await, expected);
}

/// A type with no mapping keeps its bytes and says what it was, instead of failing the read.
#[tokio::test]
async fn unmapped_types() {
    let one_two = hex_bytes("00000002000000170000000400000001000000170000000400000002");
    let three_null = hex_bytes("0000000200000017000000040000000300000017ffffffff");

    let expected = batch(
        vec![
            nullable("mac", DataType::Binary)
                .with_extension_type(Opaque::new("macaddr", "PostgreSQL")),
            nullable("point", DataType::Binary)
                .with_extension_type(Opaque::new("it_point", "PostgreSQL")),
        ],
        vec![
            Arc::new(BinaryArray::from(vec![
                Some(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0x03][..]),
                Some(&[0xff; 6][..]),
                None,
            ])),
            // PG's record framing: field count, then each field's type OID, byte length and bytes.
            // `int4` is OID 23, and a null field is a length of -1 followed by nothing.
            Arc::new(BinaryArray::from(vec![
                Some(one_two.as_slice()),
                Some(three_null.as_slice()),
                None,
            ])),
        ],
    );

    assert_eq!(read_table("it_opaque").await, expected);
}
