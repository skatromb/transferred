//! The `transferred.pg_range` Arrow extension type.
//!
//! A Postgres range is a pair of bounds plus a tag saying which of them are inclusive, which are
//! infinite, and whether the range holds anything at all. Arrow has no range type, so the parts
//! become struct fields.

use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType as ArrowType, Field as ArrowField, Fields as ArrowFields};
use postgres_protocol::types::{Range, RangeBound, range_from_sql};
use tokio_postgres::types::{FromSql, Type as PgType};
use transferred_core::{Result, TransferredError};

/// The bounds, then the three things a pair of bounds cannot say on its own.
pub const LOWER: &str = "lower";
pub const UPPER: &str = "upper";
pub const LOWER_INC: &str = "lower_inc";
pub const UPPER_INC: &str = "upper_inc";
pub const EMPTY: &str = "empty";

/// A Postgres range, spread over the struct fields that hold its bounds and its tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgRange;

impl PgRange {
    /// Constructs typed `ArrowFields` for our Range representation in Arrow Struct.
    #[must_use]
    pub fn fields(data_type: ArrowType) -> ArrowFields {
        vec![
            // A bound is null when it is infinite, which is not the whole range being SQL NULL.
            ArrowField::new(LOWER, data_type.clone(), true),
            ArrowField::new(UPPER, data_type, true),
            ArrowField::new(LOWER_INC, ArrowType::Boolean, false),
            ArrowField::new(UPPER_INC, ArrowType::Boolean, false),
            ArrowField::new(EMPTY, ArrowType::Boolean, false),
        ]
        .into()
    }

    /// Reads the bounds' `ArrowType` from Range Struct, erroring unless its fields are exactly
    /// what `fields` declares, order included.
    pub(crate) fn type_of(maybe_range: &ArrowType) -> std::result::Result<&ArrowType, ArrowError> {
        // The first field's type is the only candidate, so rebuild from it and compare the shapes.
        if let ArrowType::Struct(fields) = maybe_range
            && let Some(lower) = fields.first()
            && *fields == Self::fields(lower.data_type().clone())
        {
            return Ok(lower.data_type());
        }

        Err(ArrowError::InvalidArgumentError(format!(
            "`{}` needs a struct of `{LOWER}`, `{UPPER}`, `{LOWER_INC}`, `{UPPER_INC}` and \
             `{EMPTY}`, which `{maybe_range}` is not",
            Self::NAME
        )))
    }
}

impl ExtensionType for PgRange {
    const NAME: &'static str = "transferred.pg_range";

    /// Nothing to carry: the bounds' own type says which Postgres range they came from.
    type Metadata = ();

    fn metadata(&self) -> &() {
        &()
    }

    fn serialize_metadata(&self) -> Option<String> {
        None
    }

    fn deserialize_metadata(_: Option<&str>) -> std::result::Result<(), ArrowError> {
        Ok(())
    }

    fn supports_data_type(&self, maybe_range: &ArrowType) -> std::result::Result<(), ArrowError> {
        Self::type_of(maybe_range).map(|_| ())
    }

    fn try_new(maybe_range: &ArrowType, (): ()) -> std::result::Result<Self, ArrowError> {
        Self.supports_data_type(maybe_range).map(|()| Self)
    }
}

/// One Postgres range as PG sent it, bounds decoded but not yet reshaped for Arrow.
pub struct Bounds<T> {
    pub lower: Option<T>,
    pub upper: Option<T>,
    pub lower_inc: bool,
    pub upper_inc: bool,
    pub empty: bool,
}

impl<T> Bounds<T> {
    /// Decodes one range: the tag byte, then whichever bounds it says are there.
    pub fn from_binary<'a>(bound_type: &PgType, bytes: &'a [u8]) -> Result<Self>
    where
        T: FromSql<'a>,
    {
        let (lower, upper) = match range_from_sql(bytes).map_err(TransferredError::source)? {
            Range::Empty => {
                return Ok(Self {
                    lower: None,
                    upper: None,
                    lower_inc: false,
                    upper_inc: false,
                    empty: true,
                });
            }
            Range::Nonempty(lower, upper) => (lower, upper),
        };

        Ok(Self {
            lower: bound(bound_type, &lower)?,
            upper: bound(bound_type, &upper)?,
            lower_inc: matches!(lower, RangeBound::Inclusive(_)),
            upper_inc: matches!(upper, RangeBound::Inclusive(_)),
            empty: false,
        })
    }
}

/// Decodes a bound's value, `None` when the bound is infinite.
fn bound<'a, T: FromSql<'a>>(
    bound_type: &PgType,
    bound: &RangeBound<Option<&'a [u8]>>,
) -> Result<Option<T>> {
    let (RangeBound::Inclusive(value) | RangeBound::Exclusive(value)) = bound else {
        return Ok(None);
    };

    // Postgres rejects a NULL bound, so a bound with no value is one it did not send.
    value
        .map(|bytes| T::from_sql(bound_type, bytes))
        .transpose()
        .map_err(TransferredError::source)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn int4_range() -> ArrowType {
        ArrowType::Struct(PgRange::fields(ArrowType::Int32))
    }

    /// Spelled out rather than built from the constants: both directions fill this struct
    /// positionally, and every other test derives its expectation from `fields` too, so nothing
    /// else would notice the tag fields swapping places.
    #[test]
    fn declares_its_fields_in_one_fixed_order() {
        let fields = PgRange::fields(ArrowType::Int32);
        let names: Vec<&str> = fields.iter().map(|field| field.name().as_str()).collect();

        assert_eq!(names, ["lower", "upper", "lower_inc", "upper_inc", "empty"]);
    }

    #[test]
    fn names_the_bounds_it_was_given() {
        assert_eq!(PgRange::type_of(&int4_range()).unwrap(), &ArrowType::Int32);
    }

    #[test]
    fn rejects_a_struct_missing_the_tag_fields() {
        let bounds_only = ArrowType::Struct(
            vec![
                ArrowField::new(LOWER, ArrowType::Int32, true),
                ArrowField::new(UPPER, ArrowType::Int32, true),
            ]
            .into(),
        );
        assert!(PgRange::type_of(&bounds_only).is_err());
    }

    /// A range is defined over a single type, so bounds that disagree are no range at all.
    #[test]
    fn rejects_bounds_that_disagree_on_their_type() {
        let mismatched = ArrowType::Struct(
            vec![
                ArrowField::new(LOWER, ArrowType::Int32, true),
                ArrowField::new(UPPER, ArrowType::Int64, true),
                ArrowField::new(LOWER_INC, ArrowType::Boolean, false),
                ArrowField::new(UPPER_INC, ArrowType::Boolean, false),
                ArrowField::new(EMPTY, ArrowType::Boolean, false),
            ]
            .into(),
        );

        assert!(PgRange::type_of(&mismatched).is_err());
    }

    /// The lookup positions in `range_values` rest on this: a right-named struct in the wrong order
    /// is no `transferred.pg_range` at all, so field name and position can never disagree there.
    #[test]
    fn rejects_the_tag_fields_out_of_order() {
        let swapped = ArrowType::Struct(
            vec![
                ArrowField::new(LOWER, ArrowType::Int32, true),
                ArrowField::new(UPPER, ArrowType::Int32, true),
                ArrowField::new(UPPER_INC, ArrowType::Boolean, false),
                ArrowField::new(LOWER_INC, ArrowType::Boolean, false),
                ArrowField::new(EMPTY, ArrowType::Boolean, false),
            ]
            .into(),
        );

        assert!(PgRange::type_of(&swapped).is_err());
    }

    #[test]
    fn rejects_a_storage_type_that_is_not_a_struct() {
        assert!(PgRange.supports_data_type(&int4_range()).is_ok());
        assert!(PgRange.supports_data_type(&ArrowType::Int32).is_err());
    }

    /// `[1,5]` over a discrete type reaches us canonicalised to `[1,6)`, tag bits and all.
    #[test]
    fn decodes_a_bounded_range() {
        let bytes = [0b0000_0010, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 6];
        let bounds = Bounds::<i32>::from_binary(&PgType::INT4, &bytes).unwrap();

        assert_eq!((bounds.lower, bounds.upper), (Some(1), Some(6)));
        assert!(bounds.lower_inc && !bounds.upper_inc && !bounds.empty);
    }

    /// Both bounds infinite: no value follows the tag, and neither bound counts as inclusive.
    #[test]
    fn decodes_an_unbounded_range() {
        let bounds = Bounds::<i32>::from_binary(&PgType::INT4, &[0b0001_1000]).unwrap();

        assert_eq!((bounds.lower, bounds.upper), (None, None));
        assert!(!bounds.lower_inc && !bounds.upper_inc && !bounds.empty);
    }

    /// Empty is the one state the bounds cannot express, which is why it gets a field of its own.
    #[test]
    fn decodes_an_empty_range() {
        let bounds = Bounds::<i32>::from_binary(&PgType::INT4, &[0b0000_0001]).unwrap();

        assert_eq!((bounds.lower, bounds.upper), (None, None));
        assert!(bounds.empty);
    }

    #[test]
    fn rejects_bytes_that_are_not_a_range() {
        assert!(Bounds::<i32>::from_binary(&PgType::INT4, &[]).is_err());
    }
}
