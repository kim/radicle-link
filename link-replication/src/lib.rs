// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

#![allow(private_intra_doc_links, incomplete_features)]
#![warn(clippy::extra_unused_lifetimes)]
#![deny(broken_intra_doc_links)]
#![feature(
    bool_to_option,
    generic_associated_types,
    never_type,
    unwrap_infallible
)]

use std::fmt::Debug;

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate tracing;

use link_crypto::PeerId;

pub mod error;
pub use error::{Error, ErrorBox};

pub mod fetch;
pub mod internal;
pub mod io;
pub mod peek;
pub mod refs;

mod eval;

mod ids;
pub use ids::{Identities, LocalIdentity, Urn, VerifiedIdentity};

mod odb;
pub use odb::Odb;

mod refdb;
pub use refdb::{Applied, Policy, Refdb, SymrefTarget, Update, Updated};

mod sigrefs;
pub use sigrefs::{SignedRefs, Sigrefs};

mod state;
use state::FetchState;

mod success;
pub use success::Success;

mod track;
pub use track::Tracking;

mod transmit;
pub use transmit::{FilteredRef, Negotiation, Net, SkippedFetch, WantsHaves};

mod validation;
pub use validation::validate;

// Re-exports
pub use git_repository::refs::{namespace, Namespace};
pub use link_git_protocol::{oid, ObjectId};

pub trait LocalPeer {
    fn id(&self) -> &PeerId;
}

#[tracing::instrument(skip(cx, whoami), fields(local_id = %LocalPeer::id(cx)))]
pub fn pull<C>(
    cx: &mut C,
    remote_id: PeerId,
    whoami: Option<LocalIdentity>,
) -> Result<Success<<C as Identities>::Urn>, Error>
where
    C: Identities
        + LocalPeer
        + Net
        + Refdb
        + SignedRefs<Oid = <C as Identities>::Oid>
        + Tracking<Urn = <C as Identities>::Urn>,
    <C as Identities>::Oid: Debug + PartialEq + Send + Sync + 'static,
    <C as Identities>::Urn: Clone + Debug + Ord,
{
    if LocalPeer::id(cx) == &remote_id {
        return Err("cannot replicate from self".into());
    }
    let anchor = ids::current(cx)?.ok_or("pull: missing `rad/id`")?;
    eval::pull(&mut FetchState::default(), cx, anchor, remote_id, whoami)
}

#[tracing::instrument(skip(cx, whoami), fields(local_id = %LocalPeer::id(cx)))]
pub fn clone<C>(
    cx: &mut C,
    remote_id: PeerId,
    whoami: Option<LocalIdentity>,
) -> Result<Success<<C as Identities>::Urn>, Error>
where
    C: Identities
        + LocalPeer
        + Net
        + Refdb
        + SignedRefs<Oid = <C as Identities>::Oid>
        + Tracking<Urn = <C as Identities>::Urn>,
    <C as Identities>::Oid: Debug + PartialEq + Send + Sync + 'static,
    <C as Identities>::Urn: Clone + Debug + Ord,
{
    info!("fetching initial verification refs");
    if LocalPeer::id(cx) == &remote_id {
        return Err("cannot replicate from self".into());
    }
    let mut state = FetchState::default();
    eval::step(&mut state, cx, peek::ForClone { remote_id })?;
    let anchor = Identities::verify(
        cx,
        state
            .id_tip(&remote_id)
            .expect("BUG: peek step must ensure we got a rad/id ref"),
        state.lookup_delegations(&remote_id),
    )?;
    eval::pull(&mut state, cx, anchor, remote_id, whoami)
}
