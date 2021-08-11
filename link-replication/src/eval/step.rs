// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use futures_lite::future::block_on;
use tracing::Instrument as _;

use crate::{
    ids,
    internal::{Layout, UpdateTips},
    refs,
    state::FetchState,
    Error,
    Identities,
    Negotiation,
    Net,
    Refdb,
    SkippedFetch,
};

pub(crate) fn step<U, C, S>(
    state: &mut FetchState<U>,
    cx: &mut C,
    step: S,
) -> Result<(S, Option<SkippedFetch>), Error>
where
    U: ids::Urn + Ord,
    C: Identities<Urn = U> + Net + Refdb,
    S: Layout + Negotiation + UpdateTips + Send + Sync + 'static,
{
    Refdb::reload(cx)?;
    let (step, res) = block_on(Net::run_fetch(cx, step).in_current_span())?;
    if let Ok(refs) = &res {
        Layout::pre_validate(&step, refs)?;
        for r in refs {
            if let Some(rad) = r.parsed.as_ref().left() {
                match rad {
                    refs::parsed::Rad::Id => {
                        state.insert_id_tip(r.remote_id, r.tip);
                    },

                    refs::parsed::Rad::Ids { urn } => {
                        if let Ok(urn) = C::Urn::try_from_id(urn) {
                            state.insert_delegation_tip(r.remote_id, urn, r.tip);
                        }
                    },

                    refs::parsed::Rad::SignedRefs => {
                        state.insert_sigref_tip(r.remote_id, r.tip);
                    },

                    _ => {},
                }
            }
        }

        state.update_all(
            UpdateTips::prepare(&step, state, cx, refs)?
                .into_iter()
                .map(|u| u.into_owned()),
        );
    }

    Ok((step, res.err()))
}
