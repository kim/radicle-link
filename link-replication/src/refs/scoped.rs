// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

//! Ref rewriting utilities.
//!
//! Note that this is an internal API, exported mainly for testing. In
//! particular, ref name parameters are generally expected to be pre-validated
//! in some way, and should never be empty.

use std::{
    fmt::{self, Display},
    iter,
    ops::Deref,
};

use either::{
    Either,
    Either::{Left, Right},
};
use git_ref_format::{name, Component, RefStr, RefString};
use link_crypto::PeerId;

use super::{from_peer_id, lit, parsed};
use crate::Urn;

pub use git_ref_format::{Namespaced, Qualified};

/// Add a (`link`) namespace (of type [`Urn`] to `name`.
///
/// `name` should not already be namespaced, but this condition is not checked.
/// It is thus possible for the returned [`Namespaced`] to have more than one
/// namespace.
pub fn namespaced<'a, U, Q>(ns: &U, name: Q) -> Namespaced<'a>
where
    U: Urn,
    Q: Into<Qualified<'a>>,
{
    let ns = Component::from_refstring(super::from_urn(ns))
        .expect("urn is a valid component")
        .to_owned();
    name.into().add_namespace(ns)
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub struct RemoteTracking<'a>(Qualified<'a>);

/// Ensure that the ref `name` is a remote tracking branch.
///
/// If `name` starts with "refs/remotes/", this is the identity function.
/// Otherwise, "refs/remotes/`remote_id`/" is prepended. Note that the `link`
/// naming convention applies, which, unlike standard git, mandates a category
/// after the remote name. Eg.:
///
///     refs/remotes/xyz/heads/main
///     refs/remotes/xyz/rad/id
///
/// If `name` is not a remote tracking branch (ie. does not start with
/// "refs/remotes/"), it must have at least three components, so as to enforce
/// that it has a category (eg. "refs/heads/main").
///
/// If `name` is namespaced (ie. it starts with "refs/namespaces/"), namespaces
/// are stripped recursively.
pub fn remote_tracking<'a>(
    remote_id: &PeerId,
    name: impl Into<Qualified<'a>>,
) -> Option<RemoteTracking<'a>> {
    let name = name.into();
    let mut iter = name.components();
    {
        let refs = iter.next().and_then(|c| c.as_lit())?;
        debug_assert_eq!(lit::Refs, refs, "`Qualified` must start with 'refs/'");
    }
    match (iter.next()?.as_str(), iter.next()?) {
        (name::str::REMOTES, _) => {
            let _cat = iter.next()?;
            let _name = iter.next()?;
            let q = Qualified::from((
                lit::Refs,
                lit::Remotes,
                from_peer_id(remote_id).and(name.components().skip(3).collect::<RefString>()),
            ));
            Some(RemoteTracking(q))
        },
        (name::str::NAMESPACES, _) => remote_tracking(
            remote_id,
            name.namespaced()
                .expect("name is namespaced")
                .strip_namespace_recursive(),
        ),
        (_, _) => {
            // SAFETY: pattern match proves that there are at least two
            // components after "refs".
            let rest = name.components().skip(1).collect::<RefString>();
            let q = Qualified::from((lit::Refs, lit::Remotes, from_peer_id(remote_id).and(rest)));
            Some(RemoteTracking(q))
        },
    }
}

impl<'a> Deref for RemoteTracking<'a> {
    type Target = Qualified<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> AsRef<Qualified<'a>> for RemoteTracking<'a> {
    fn as_ref(&self) -> &Qualified<'a> {
        self
    }
}

impl AsRef<RefStr> for RemoteTracking<'_> {
    fn as_ref(&self) -> &RefStr {
        self
    }
}

impl<'a> From<RemoteTracking<'a>> for Qualified<'a> {
    fn from(rt: RemoteTracking<'a>) -> Self {
        rt.0
    }
}

impl Display for RemoteTracking<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Owned<'a>(Qualified<'a>);

impl<'a> Owned<'a> {
    pub fn into_remote_tracking<'b>(self, remote_id: &PeerId) -> RemoteTracking<'b> {
        // SAFETY: `Qualified` is guaranteed to start with "refs"
        let name = self.0.components().skip(1).collect::<RefString>();
        RemoteTracking(Qualified::from((
            lit::Refs,
            lit::Remotes,
            from_peer_id(remote_id).and(name),
        )))
    }

    pub fn into_owned<'b>(self) -> Owned<'b> {
        Owned(self.0.into_owned())
    }
}

/// Ensure that `name` is not a remote tracking branch.
///
/// Essentially removes "refs/remotes/*/" from `name`. Note that the `link`
/// naming convention applies, which, unlike standard git, mandates a category
/// after the remote name. Eg.:
///
///     refs/remotes/xyz/heads/main
///     refs/remotes/xyz/rad/id
///
/// Returns `None` if:
///
/// * `name` is namespaced (ie. starts with  "refs/namespaces/")
/// * less then two components are found after "refs/remotes/*/"
/// * `name` is an owned ref, but does not have a category. In other words, if
///   `name` does not start with "refs/namespaces/" nor "refs/remotes/", but has
///   less than three components.
pub fn owned(name: Qualified) -> Option<Owned> {
    let mut iter = name.components();
    {
        let refs = iter.next().and_then(|c| c.as_lit())?;
        debug_assert_eq!(lit::Refs, refs, "`Qualified` must start with 'refs/'");
    }
    match (iter.next()?.as_str(), iter.next()?) {
        (name::str::NAMESPACES, _) => None,
        (name::str::REMOTES, _) => {
            let cat = iter.next()?;
            let name = iter
                .next()
                .map(|x| iter::once(x).chain(iter).collect::<RefString>())?;
            let q = Qualified::from((lit::Refs, cat, name));
            Some(Owned(q))
        },
        (_, _) => Some(Owned(name)),
    }
}

impl<'a> Deref for Owned<'a> {
    type Target = Qualified<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<RefStr> for Owned<'_> {
    fn as_ref(&self) -> &RefStr {
        self
    }
}

impl<U> From<parsed::Rad<U>> for Owned<'_>
where
    U: Urn,
{
    fn from(r: parsed::Rad<U>) -> Self {
        Self(r.into())
    }
}

impl<'a> From<Owned<'a>> for Qualified<'a> {
    fn from(o: Owned<'a>) -> Self {
        o.0
    }
}

impl Display for Owned<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub struct Scoped<'a, 'b> {
    scope: &'a PeerId,
    name: Either<Owned<'b>, RemoteTracking<'b>>,
}

/// Conditionally ensure `name` is either a remote tracking branch or not.
///
/// If the `wanted_id` is equal to the `remote_id`, the result is not a remote
/// tracking branch, otherwise it is. For example, given the name:
///
///     refs/heads/main
///
/// If `wanted_id == remote_id`, the result is:
///
///     refs/heads/main
///
/// Otherwise
///
///     refs/remotes/<wanted_id>/heads/main
///
/// This is used to determine the right "scope" of a ref when fetching from
/// `remote_id`. `name` should generally not be a remote tracking branch itself,
/// as that information is stripped.
pub fn scoped<'a, 'b>(
    wanted_id: &'a PeerId,
    remote_id: &PeerId,
    name: impl Into<Qualified<'b>>,
) -> Scoped<'a, 'b> {
    let name = name.into();
    let own = owned(name).expect("BUG: `scoped` should receive valid refs");
    Scoped {
        scope: wanted_id,
        name: if wanted_id == remote_id {
            Left(own)
        } else {
            Right(own.into_remote_tracking(wanted_id))
        },
    }
}

impl AsRef<RefStr> for Scoped<'_, '_> {
    fn as_ref(&self) -> &RefStr {
        self.name.as_ref().either(AsRef::as_ref, AsRef::as_ref)
    }
}

impl<'b> From<Scoped<'_, 'b>> for Qualified<'b> {
    fn from(s: Scoped<'_, 'b>) -> Self {
        s.name.either(Into::into, Into::into)
    }
}
