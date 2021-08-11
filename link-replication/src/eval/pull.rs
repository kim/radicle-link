// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{collections::BTreeSet, fmt::Debug, marker::PhantomData};

use super::rad;
use crate::{
    error,
    eval,
    fetch,
    ids,
    peek,
    sigrefs,
    state::FetchState,
    validate,
    Error,
    Identities,
    LocalIdentity,
    LocalPeer,
    Net,
    PeerId,
    Refdb,
    SignedRefs,
    SkippedFetch,
    Success,
    Tracking,
};

pub(crate) fn pull<U, C>(
    state: &mut FetchState<U>,
    cx: &mut C,
    anchor: C::VerifiedIdentity,
    remote_id: PeerId,
    whoami: Option<LocalIdentity>,
) -> Result<Success<<C as Identities>::Urn>, Error>
where
    U: ids::Urn + Clone + Debug + Ord,
    C: Identities<Urn = U>
        + LocalPeer
        + Net
        + Refdb
        + SignedRefs<Oid = <C as Identities>::Oid>
        + Tracking<Urn = U>,
    <C as Identities>::Oid: Debug + PartialEq + Send + Sync + 'static,
{
    use either::Either::*;

    info!("fetching verification refs");
    let (
        peek::ForFetch {
            local_id,
            remote_id,
            delegates,
            tracked,
        },
        skip,
    ) = {
        let spec = peek::for_fetch(&state.as_shim(cx), &anchor, remote_id)?;
        eval::step(state, cx, spec)?
    };

    if matches!(skip, Some(SkippedFetch::NoMatchingRefs)) {
        return Ok(Success {
            applied: Default::default(),
            requires_confirmation: false,
            validation: vec![],
            _marker: PhantomData,
        });
    }

    let delegates: BTreeSet<PeerId> = delegates
        .into_iter()
        .filter(move |id| id != &local_id)
        .collect();

    let requires_confirmation = {
        if skip.is_some() {
            false
        } else {
            info!("setting up local rad/ hierarchy");
            let shim = state.as_shim(cx);
            match ids::newest(&shim, &delegates)? {
                None => false,
                Some((their_id, theirs)) => match rad::newer(&shim, Some(anchor), theirs)? {
                    Err(error::ConfirmationRequired) => true,
                    Ok(newest) => {
                        let rad::Rad { track, up } = match newest {
                            Left(ours) => rad::setup(&shim, &local_id, &ours, whoami)?,
                            Right(theirs) => rad::setup(&shim, their_id, &theirs, whoami)?,
                        };

                        state.track_all(track);
                        state.update_all(up);

                        false
                    },
                },
            }
        }
    };

    info!("loading combined sigrefs");
    let signed_refs = sigrefs::combined(
        &state.as_shim(cx),
        sigrefs::Select {
            must: &delegates,
            may: &tracked,
            cutoff: 2,
        },
    )?;
    info!("fetching data");
    eval::step(
        state,
        cx,
        fetch::Fetch {
            local_id,
            remote_id,
            signed_refs,
        },
    )?;
    // TODO: is this necessary?
    info!("reloading combined sigrefs");
    let signed_refs = sigrefs::combined(
        &state.as_shim(cx),
        sigrefs::Select {
            must: &delegates,
            may: &tracked,
            cutoff: 2,
        },
    )?;

    info!("post-validation");
    let warnings = validate(&state.as_shim(cx), &signed_refs)?;

    info!("updating trackings");
    for (peer, urn) in state.drain_trackings() {
        Tracking::track(cx, &peer, urn.as_ref())?;
    }
    info!("updating tips");
    let applied = Refdb::update(cx, state.drain_updates())?;
    for u in &applied.updated {
        debug!("applied {:?}", u);
    }

    info!("updating signed refs");
    SignedRefs::update(cx)?;

    Ok(Success {
        applied,
        requires_confirmation,
        validation: warnings,
        _marker: PhantomData,
    })
}
