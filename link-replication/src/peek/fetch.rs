// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::collections::{BTreeSet, HashSet};

use bstr::ByteSlice as _;
use link_crypto::PeerId;
use link_git_protocol::Ref;

use super::{guard_required, mk_ref_update, ref_prefixes, required_refs};
use crate::{
    error,
    ids,
    internal::{Layout, UpdateTips},
    refs,
    FetchState,
    FilteredRef,
    Identities,
    Negotiation,
    Refdb,
    Update,
    WantsHaves,
};

#[derive(Debug)]
pub struct ForFetch {
    /// The local peer, so we don't fetch our own data.
    pub local_id: PeerId,
    /// The remote peer being fetched from.
    pub remote_id: PeerId,
    /// The set of keys the latest known identity revision delegates to.
    /// Indirect delegations are resolved.
    pub delegates: BTreeSet<PeerId>,
    /// Additional peers being tracked (ie. excluding `delegates`).
    pub tracked: BTreeSet<PeerId>,
}

impl ForFetch {
    pub fn peers(&self) -> impl Iterator<Item = &PeerId> {
        self.delegates
            .iter()
            .chain(self.tracked.iter())
            .filter(move |id| *id != &self.local_id)
    }

    pub fn required_refs(&self) -> impl Iterator<Item = refs::Scoped<'_, 'static>> {
        self.delegates
            .iter()
            .filter(move |id| *id != &self.local_id)
            .flat_map(move |id| required_refs(id, &self.remote_id))
    }
}

impl Negotiation for ForFetch {
    fn ref_prefixes(&self) -> Vec<refs::Scoped<'_, 'static>> {
        self.peers()
            .flat_map(move |id| ref_prefixes(id, &self.remote_id))
            .collect()
    }

    fn ref_filter(&self, r: Ref) -> Option<FilteredRef<Self>> {
        use refs::parsed::Identity;

        let (name, tip) = refs::into_unpacked(r);
        let parsed = refs::parse::<Identity>(name.as_bstr())?;

        match parsed.remote {
            Some(remote_id) if remote_id == self.local_id => None,
            Some(remote_id) => Some(FilteredRef::new(name, tip, &remote_id, parsed)),
            None => Some(FilteredRef::new(name, tip, &self.remote_id, parsed)),
        }
    }

    fn wants_haves<R: Refdb>(
        &self,
        db: &R,
        refs: impl IntoIterator<Item = FilteredRef<Self>>,
    ) -> Result<WantsHaves<Self>, R::FindError> {
        let mut wanted = HashSet::new();
        let mut wants = BTreeSet::new();
        let mut haves = BTreeSet::new();

        let peers: BTreeSet<&PeerId> = self.peers().collect();
        for r in refs {
            let refname = refs::remote_tracking(&r.remote_id, r.name.as_bstr());
            if let Some(oid) = db.refname_to_id(&refname)? {
                haves.insert(oid.into());
            }

            if peers.contains(&r.remote_id) {
                wants.insert(r.tip);
                wanted.insert(r);
            }
        }

        Ok(WantsHaves {
            wanted,
            wants,
            haves,
        })
    }
}

impl UpdateTips for ForFetch {
    fn prepare<'a, U, I>(
        &self,
        s: &FetchState<U>,
        ids: &I,
        refs: &'a [FilteredRef<Self>],
    ) -> Result<Vec<Update<'a>>, error::Prepare<I::VerificationError>>
    where
        U: ids::Urn + Ord,
        I: Identities<Urn = U>,
    {
        let mut updates = Vec::new();
        for r in refs {
            debug_assert!(r.remote_id != self.local_id, "never touch our own");
            let is_delegate = self.delegates.contains(&r.remote_id);
            // XXX: we should verify all ids at some point, but non-delegates
            // would be a warning only
            if is_delegate && r.name.ends_with(b"rad/id") {
                Identities::verify(ids, r.tip, s.lookup_delegations(&r.remote_id))
                    .map_err(error::Prepare::Verification)?;
            }
            if let Some(u) = mk_ref_update::<_, I::Urn>(r) {
                updates.push(u)
            }
        }

        Ok(updates)
    }
}

impl Layout for ForFetch {
    fn pre_validate(&self, refs: &[FilteredRef<Self>]) -> Result<(), error::Layout> {
        guard_required(
            self.required_refs().collect(),
            refs.iter()
                .map(|x| refs::scoped(&x.remote_id, &self.remote_id, x.name.as_bstr()))
                .collect(),
        )
    }
}
