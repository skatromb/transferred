//! The `transferred.pg_range` Arrow extension type.
//!
//! A Postgres range is a pair of bounds plus a tag saying which of them are inclusive, which are
//! infinite, and whether the range holds anything at all. Arrow has no range type, so the parts
//! become struct fields.

use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType as ArrowType, Field as ArrowField, Fields as ArrowFields};

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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;

    use super::*;

    fn int4_range() -> ArrowType {
        ArrowType::Struct(PgRange::fields(ArrowType::Int32))
    }

    /// Literal names, not the constants: every other test derives its expectation from `fields`.
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

    /// `range_values` looks fields up by position, which only holds if a reordered struct is refused.
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

    /// A tag over the wrong storage type must not produce a `PgRange` the readers then trust.
    #[test]
    fn refuses_to_rebuild_over_the_wrong_storage_type() {
        let mut field = ArrowField::new("valid", ArrowType::Int32, true);
        field.set_metadata([(EXTENSION_TYPE_NAME_KEY.to_owned(), PgRange::NAME.to_owned())].into());

        assert!(field.try_extension_type::<PgRange>().is_err());
    }
}
