// Copyright © 2022 The Radicle Link Contributors
// Copyright © 2019 Laurențiu Nicola <lnicola@dend.ro>
// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    collections::HashMap,
    convert::TryFrom,
    env,
    io,
    os::unix::{io::RawFd, prelude::FromRawFd},
    process,
};

use itertools::Itertools as _;
use nix::fcntl::{fcntl, FcntlArg::F_SETFD, FdFlag};

use super::{io_other, Sockets};

/// Environment variable containing colon-separated list of names corresponding
/// to the `FileDescriptorName` option in the systemd service file.
const LISTEN_FDNAMES: &str = "LISTEN_FDNAMES";

/// Load any sockets which can be loaded from the environment.
///
/// This will check for the environment variables LISTEN_FDNAMES and
/// LISTEN_FDS, which are set by systemd. If neither of these
/// environment variables are set this will return `None`, otherwise it
/// will load each socket and return a `HashMap` from the LISTEN_FDNAMES
/// entry to the LISTEN_FDS entry. This is similar to the behavior of
/// `sd_listen_fds_with_names()`[1]
///
/// This function will return an error if
/// * LISTEN_PID is not an integer
/// * LISTEN_FDS is not an integer
/// * The number of sockets indicated by LISTEN_FDS is not the same as the
///   number of names in LISTEN_FDNAMES
///
/// [1]: https://www.freedesktop.org/software/systemd/man/sd_listen_fds.html
pub struct Systemd {
    fds: HashMap<String, RawFd>,
}

impl Systemd {
    pub fn from_env() -> io::Result<Self> {
        let fds = listen_fds()?
            .zip_longest(env::var(LISTEN_FDNAMES).unwrap_or_default().split(':'))
            .map(|item| {
                item.both()
                    .map(|(fd, name)| (name.to_owned(), fd))
                    .ok_or_else(|| io_other("LISTEN_FDNAMES and LISTEN_FDS do not match"))
            })
            .collect::<Result<_, _>>()?;

        Ok(Self { fds })
    }
}

impl Sockets for Systemd {
    fn activate(&mut self, name: &str) -> io::Result<Vec<socket2::Socket>> {
        Ok(self
            .fds
            .remove(name)
            .map(|fd| {
                let sock = unsafe { socket2::Socket::from_raw_fd(fd) };
                vec![sock]
            })
            .unwrap_or_default())
    }
}

/// Checks for file descriptors passed by the service manager for socket
/// activation.
///
/// The function returns an iterator over file descriptors, starting from
/// `SD_LISTEN_FDS_START`. The number of descriptors is obtained from the
/// `LISTEN_FDS` environment variable.
///
/// Before returning, the file descriptors are set as `O_CLOEXEC`.
///
/// See [`sd_listen_fds(3)`][sd_listen_fds] for details.
///
/// # Attribution
///
/// Authored by Laurențiu Nicola <lnicola@dend.ro>, released under MIT OR
/// Apache-2.0 at:
///
/// <https://github.com/lnicola/sd-notify/blob/7e9325902b2f44c1e9dc5dc7ca467791207fbfae/src/lib.rs#L163>
///
/// [sd_listen_fds]: https://www.freedesktop.org/software/systemd/man/sd_listen_fds.html
pub fn listen_fds() -> io::Result<impl Iterator<Item = RawFd>> {
    struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            env::remove_var("LISTEN_PID");
            env::remove_var("LISTEN_FDS");
        }
    }

    let _guard = Guard;

    let listen_pid = if let Ok(pid) = env::var("LISTEN_PID") {
        pid
    } else {
        return Ok(0..0);
    }
    .parse::<u32>()
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid LISTEN_PID"))?;

    if listen_pid != process::id() {
        return Ok(0..0);
    }

    let listen_fds = if let Ok(fds) = env::var("LISTEN_FDS") {
        fds
    } else {
        return Ok(0..0);
    }
    .parse::<u32>()
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid LISTEN_FDS"))?;

    let overflow = || io::Error::new(io::ErrorKind::InvalidInput, "fd count overflowed");

    const SD_LISTEN_FDS_START: u32 = 3;
    let last = SD_LISTEN_FDS_START
        .checked_add(listen_fds)
        .ok_or_else(overflow)?;

    for fd in SD_LISTEN_FDS_START..last {
        // Set FD_CLOEXEC to avoid further inheritance to children.
        let fd = RawFd::try_from(fd).map_err(|_| overflow())?;
        fcntl(fd, F_SETFD(FdFlag::FD_CLOEXEC))?;
    }

    let last = RawFd::try_from(last).map_err(|_| overflow())?;
    let listen_fds = SD_LISTEN_FDS_START as RawFd..last;
    Ok(listen_fds)
}
