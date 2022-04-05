// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

//! Implementation of the systemd socket activation protocol.
//! <http://0pointer.de/blog/projects/socket-activation.html>

use std::io;

mod sd;
pub use sd::Systemd;

#[cfg(target_os = "macos")]
mod ld;
#[cfg(target_os = "macos")]
pub use ld::Launchd;

pub trait Sockets {
    fn activate(&mut self, name: &str) -> io::Result<Vec<socket2::Socket>>;
}

#[cfg(target_os = "macos")]
pub fn default() -> io::Result<impl Sockets> {
    Ok(Launchd)
}

#[cfg(not(target_os = "macos"))]
pub fn default() -> io::Result<impl Sockets> {
    Systemd::from_env()
}

fn io_other<E>(error: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, error)
}
