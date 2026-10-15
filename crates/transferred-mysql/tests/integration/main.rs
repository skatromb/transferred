//! Tests MySQL transfers against disposable servers.
#![cfg(test)]

mod arrow_input;
mod batch_source;
mod bulk;
mod constraints;
mod container;
mod contracts;
mod destination;
mod gated_source;
mod interchange;
mod native_types;
mod postgres_server;
mod publication;
mod server;
mod source;
mod tls;
mod wire_fault;
mod wire_proxy;
mod wire_state;
