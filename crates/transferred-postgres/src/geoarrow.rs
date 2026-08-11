//! The `geoarrow.wkb` Arrow extension type.
//!
//! Hand-rolled rather than taken from `geoarrow-schema`, whose latest release still pins
//! `arrow-schema` 58: its `ExtensionType` impls would be for a different crate than our `Field`.

// TODO(0.2.0): drop this module for `geoarrow-schema` once it releases against `arrow-schema` 59.
// PLAN.md records what the swap costs and why the release buys no capability of its own.

use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType};
use serde_json::{Map, Value};

/// How every `PostGIS` SRID this maps is named. Its `spatial_ref_sys` is a plain table, so a
/// user-defined SRID may belong to another authority, which 0.1 does not look up.
const EPSG: &str = "EPSG:";

/// `crs_type` for a coordinate system named by authority and code, rather than spelled out inline.
const AUTHORITY_CODE: &str = "authority_code";

/// `edges` of a coordinate system whose lines follow great circles.
const SPHERICAL: &str = "spherical";

/// Geometry as WKB bytes, carrying the coordinate system and edge interpretation of its column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wkb {
    /// EPSG code of the coordinate system; absent when the column's rows may disagree on one.
    epsg: Option<i32>,
    /// Whether edges follow great circles rather than running straight.
    spherical: bool,
}

impl Wkb {
    /// Geometry on a plane, as PG `geometry` measures it.
    #[must_use]
    pub fn planar(epsg: Option<i32>) -> Self {
        Self {
            epsg,
            spherical: false,
        }
    }

    /// Geometry on a globe, as PG `geography` measures it.
    #[must_use]
    pub fn spherical(epsg: Option<i32>) -> Self {
        Self {
            epsg,
            spherical: true,
        }
    }

    /// EPSG code the whole column shares, if it declares one.
    #[must_use]
    pub fn epsg(&self) -> Option<i32> {
        self.epsg
    }

    /// Whether edges follow great circles, which is what separates `geography` from `geometry`.
    #[must_use]
    pub fn is_spherical(&self) -> bool {
        self.spherical
    }
}

impl ExtensionType for Wkb {
    const NAME: &'static str = "geoarrow.wkb";

    /// The type is nothing but its metadata: two optional keys of the `geoarrow.wkb` object.
    type Metadata = Self;

    fn metadata(&self) -> &Self {
        self
    }

    fn serialize_metadata(&self) -> Option<String> {
        let mut object = Map::new();

        if let Some(epsg) = self.epsg {
            object.insert("crs".to_owned(), format!("{EPSG}{epsg}").into());
            object.insert("crs_type".to_owned(), AUTHORITY_CODE.into());
        }
        if self.spherical {
            object.insert("edges".to_owned(), SPHERICAL.into());
        }

        // Every key is optional, and an empty object would only say we know nothing.
        (!object.is_empty()).then(|| Value::Object(object).to_string())
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self, ArrowError> {
        let Some(metadata) = metadata else {
            return Ok(Self::planar(None));
        };

        let object: Value = serde_json::from_str(metadata).map_err(|error| {
            ArrowError::ParseError(format!("`{}` metadata `{metadata}`: {error}", Self::NAME))
        })?;

        Ok(Self {
            epsg: epsg(&object),
            spherical: text(&object, "edges") == Some(SPHERICAL),
        })
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        matches!(
            data_type,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        )
        .then_some(())
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "`{}` holds WKB bytes, which `{data_type}` cannot",
                Self::NAME
            ))
        })
    }

    fn try_new(data_type: &DataType, metadata: Self) -> Result<Self, ArrowError> {
        metadata.supports_data_type(data_type)?;
        Ok(metadata)
    }
}

/// Read an EPSG code back out, ignoring any coordinate system spelled another way — the ones
/// Postgres could not be told about either.
fn epsg(object: &Value) -> Option<i32> {
    // The spec omits `crs_type` exactly when the producer cannot vouch for the value. A declared
    // SRID is enforced on every row, so trusting one would fail the load, not just the column.
    if text(object, "crs_type") != Some(AUTHORITY_CODE) {
        return None;
    }

    text(object, "crs")?.strip_prefix(EPSG)?.parse().ok()
}

/// A string-valued key of the metadata object, if it holds one.
fn text<'a>(object: &'a Value, key: &str) -> Option<&'a str> {
    object.get(key)?.as_str()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    /// Serialize then read back, which is the whole contract a destination relies on.
    fn round_trip(wkb: &Wkb) -> Wkb {
        Wkb::deserialize_metadata(wkb.serialize_metadata().as_deref()).unwrap()
    }

    #[test]
    fn names_the_coordinate_system_by_authority_code() {
        let wkb = Wkb::planar(Some(4326));
        assert_eq!(
            wkb.serialize_metadata().unwrap(),
            r#"{"crs":"EPSG:4326","crs_type":"authority_code"}"#
        );
        assert_eq!(round_trip(&wkb), wkb);
    }

    #[test]
    fn marks_spherical_edges() {
        let wkb = Wkb::spherical(Some(4326));
        assert_eq!(
            wkb.serialize_metadata().unwrap(),
            r#"{"crs":"EPSG:4326","crs_type":"authority_code","edges":"spherical"}"#
        );
        assert_eq!(round_trip(&wkb), wkb);
    }

    /// A column whose rows may each carry their own SRID has nothing to say at column level.
    #[test]
    fn omits_metadata_entirely_without_a_coordinate_system() {
        let wkb = Wkb::planar(None);
        assert_eq!(wkb.serialize_metadata(), None);
        assert_eq!(round_trip(&wkb), wkb);
    }

    #[test]
    fn spherical_edges_survive_without_a_coordinate_system() {
        let wkb = Wkb::spherical(None);
        assert_eq!(
            wkb.serialize_metadata().unwrap(),
            r#"{"edges":"spherical"}"#
        );
        assert_eq!(round_trip(&wkb), wkb);
    }

    /// The spec allows PROJJSON and WKT too; neither can be handed to Postgres as an SRID.
    #[test]
    fn ignores_a_coordinate_system_not_named_by_authority_code() {
        let projjson = r#"{"crs":{"type":"GeographicCRS"},"crs_type":"projjson"}"#;
        assert_eq!(
            Wkb::deserialize_metadata(Some(projjson)).unwrap(),
            Wkb::planar(None)
        );
    }

    /// An omitted `crs_type` is the spec's way of saying the producer cannot vouch for the value,
    /// and Postgres enforces a declared SRID per row — so guessing here would fail whole loads.
    #[test]
    fn ignores_a_coordinate_system_nothing_vouches_for() {
        assert_eq!(
            Wkb::deserialize_metadata(Some(r#"{"crs":"EPSG:4326"}"#)).unwrap(),
            Wkb::planar(None)
        );
    }

    #[test]
    fn rejects_a_storage_type_that_cannot_hold_bytes() {
        assert!(
            Wkb::planar(None)
                .supports_data_type(&DataType::Binary)
                .is_ok()
        );
        assert!(
            Wkb::planar(None)
                .supports_data_type(&DataType::Utf8)
                .is_err()
        );
    }

    #[test]
    fn rejects_malformed_metadata() {
        assert!(Wkb::deserialize_metadata(Some("{")).is_err());
    }
}
