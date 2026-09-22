//! `geoarrow.wkb` as `PostGIS` reads it: the EPSG code a column shares, and edges on a plane or a globe.

use std::sync::Arc;

use geoarrow_schema::{Crs, CrsType, Edges, Metadata, WkbType};

/// How every `PostGIS` SRID this maps is named. Its `spatial_ref_sys` is a plain table, so a
/// user-defined SRID may belong to another authority, which 0.1 does not look up.
const EPSG: &str = "EPSG:";

/// Tags geometry on a plane, as PG `geometry` measures it.
#[must_use]
pub fn planar(epsg: Option<i32>) -> WkbType {
    wkb(epsg, None)
}

/// Tags geometry on a globe, as PG `geography` measures it.
#[must_use]
pub fn spherical(epsg: Option<i32>) -> WkbType {
    wkb(epsg, Some(Edges::Spherical))
}

fn wkb(epsg: Option<i32>, edges: Option<Edges>) -> WkbType {
    let crs = epsg.map_or_else(Crs::default, |code| {
        Crs::from_authority_code(format!("{EPSG}{code}"))
    });
    WkbType::new(Arc::new(Metadata::new(crs, edges)))
}

/// Whether edges follow great circles, which is what separates `geography` from `geometry`.
#[must_use]
pub fn is_spherical(wkb: &WkbType) -> bool {
    wkb.metadata().edges() == Some(Edges::Spherical)
}

/// EPSG code the whole column shares, if it declares one — ignoring any coordinate system spelled
/// another way, the ones Postgres could not be told about either.
#[must_use]
pub fn epsg(wkb: &WkbType) -> Option<i32> {
    let crs = wkb.metadata().crs();

    // The spec omits `crs_type` exactly when the producer cannot vouch for the value. A declared
    // SRID is enforced on every row, so trusting one would fail the load, not just the column.
    if crs.crs_type() != Some(CrsType::AuthorityCode) {
        return None;
    }

    crs.crs_value()?.as_str()?.strip_prefix(EPSG)?.parse().ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use arrow_schema::extension::ExtensionType;

    use super::*;

    /// Reads a tag back the way a destination receives it.
    fn parse(metadata: Option<&str>) -> WkbType {
        WkbType::new(WkbType::deserialize_metadata(metadata).unwrap())
    }

    /// Serializes then reads back, which is the whole contract a destination relies on.
    fn round_trip(wkb: &WkbType) -> WkbType {
        parse(wkb.serialize_metadata().as_deref())
    }

    #[test]
    fn names_the_coordinate_system_by_authority_code() {
        let wkb = planar(Some(4326));
        assert_eq!(
            wkb.serialize_metadata().unwrap(),
            r#"{"crs":"EPSG:4326","crs_type":"authority_code"}"#
        );
        assert_eq!(epsg(&round_trip(&wkb)), Some(4326));
        assert!(!is_spherical(&round_trip(&wkb)));
    }

    #[test]
    fn marks_spherical_edges() {
        let wkb = spherical(Some(4326));
        assert_eq!(
            wkb.serialize_metadata().unwrap(),
            r#"{"crs":"EPSG:4326","crs_type":"authority_code","edges":"spherical"}"#
        );
        assert!(is_spherical(&round_trip(&wkb)));
    }

    /// A column whose rows may each carry their own SRID has nothing to say at column level.
    #[test]
    fn omits_metadata_entirely_without_a_coordinate_system() {
        let wkb = planar(None);
        assert_eq!(wkb.serialize_metadata(), None);
        assert_eq!(epsg(&round_trip(&wkb)), None);
    }

    /// Upstream spells an unknown CRS as `null` rather than leaving the key out; readers take both as none.
    #[test]
    fn spherical_edges_survive_without_a_coordinate_system() {
        let wkb = spherical(None);
        assert_eq!(
            wkb.serialize_metadata().unwrap(),
            r#"{"crs":null,"edges":"spherical"}"#
        );
        assert!(is_spherical(&round_trip(&wkb)));
        assert_eq!(epsg(&round_trip(&wkb)), None);
    }

    /// The spec allows PROJJSON and WKT too; neither can be handed to Postgres as an SRID.
    #[test]
    fn ignores_a_coordinate_system_not_named_by_authority_code() {
        let projjson = r#"{"crs":{"type":"GeographicCRS"},"crs_type":"projjson"}"#;
        assert_eq!(epsg(&parse(Some(projjson))), None);
    }

    /// An omitted `crs_type` is the spec's way of saying the producer cannot vouch for the value,
    /// and Postgres enforces a declared SRID per row — so guessing here would fail whole loads.
    #[test]
    fn ignores_a_coordinate_system_nothing_vouches_for() {
        assert_eq!(epsg(&parse(Some(r#"{"crs":"EPSG:4326"}"#))), None);
    }
}
