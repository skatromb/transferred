//! Failures injected into a real MySQL connection.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WireFault {
    Observe,
    BeforeRename,
    AfterRename,
    TimeoutRename,
    RejectRename,
    RejectBackup,
    InsertWarning,
}
