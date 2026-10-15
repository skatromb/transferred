//! MySQL `InnoDB` source and atomic full-replacement destination.

mod catalog;
mod column;
mod connection;
mod decimal;
mod dependencies;
mod destination;
mod error;
mod source;
mod staging;
mod table;
mod temporal;

pub use destination::MySqlDestination;
pub use source::MySqlSource;
