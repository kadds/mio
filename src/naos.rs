//! NaOS capability-handle integration for Mio.
//!
//! A NaOS handle is not a Unix file descriptor.  It is a waitable capability,
//! so users of this module must keep the handle alive for as long as the
//! source is registered and must perform the corresponding non-blocking
//! operation after receiving readiness.

use crate::{event::Source as EventSource, Interest, Registry, Token};

use std::io;

/// A borrowed NaOS capability handle exposed as a Mio event source.
///
/// `Source` does not own or close the handle.  The owner remains responsible
/// for closing it after deregistering the source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Source {
    handle: u64,
}

impl Source {
    /// Creates a source for a waitable NaOS capability handle.
    pub const fn new(handle: u64) -> Self {
        Self { handle }
    }

    /// Returns the underlying capability handle without transferring
    /// ownership.
    pub const fn handle(&self) -> u64 {
        self.handle
    }
}

impl EventSource for Source {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        registry.selector().register(self.handle, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        registry
            .selector()
            .reregister(self.handle, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        registry.selector().deregister(self.handle)
    }
}
