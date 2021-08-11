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
pub struct ForClone {
    pub remote_id: PeerId,
}

impl ForClone {
    pub fn required_refs(&self) -> impl Iterator<Item = refs::Scoped<'_, 'static>> {
        required_refs(&self.remote_id, &self.remote_id)
    }
}

impl Negotiation for ForClone {
    fn ref_prefixes(&self) -> Vec<refs::Scoped<'_, 'static>> {
        ref_prefixes(&self.remote_id, &self.remote_id).collect()
    }

    fn ref_filter(&self, r: Ref) -> Option<FilteredRef<Self>> {
        use either::Either::Left;
        use refs::parsed::Identity;

        let (name, tip) = refs::into_unpacked(r);
        match refs::parse::<Identity>(name.as_bstr())? {
            parsed
            @
            refs::Parsed {
                remote: None,
                inner: Left(_),
            } => Some(FilteredRef::new(name, tip, &self.remote_id, parsed)),
            _ => None,
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

        for r in refs {
            if r.remote_id != self.remote_id {
                continue;
            }
            let refname = refs::remote_tracking(&r.remote_id, r.name.as_bstr());
            if let Some(oid) = db.refname_to_id(&refname)? {
                haves.insert(oid.into());
            }

            wants.insert(r.tip);
            wanted.insert(r);
        }

        Ok(WantsHaves {
            wanted,
            wants,
            haves,
        })
    }
}

impl UpdateTips for ForClone {
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
        use ids::VerifiedIdentity as _;

        let verified = Identities::verify(
            ids,
            s.id_tip(&self.remote_id)
                .expect("BUG: `pre_validate` must ensure we got a rad/id ref"),
            s.lookup_delegations(&self.remote_id),
        )
        .map_err(error::Prepare::Verification)?;

        if verified.delegate_ids().contains(&self.remote_id) {
            Ok(refs.iter().filter_map(mk_ref_update::<_, I::Urn>).collect())
        } else {
            Ok(vec![])
        }
    }
}

impl Layout for ForClone {
    fn pre_validate(&self, refs: &[FilteredRef<Self>]) -> Result<(), error::Layout> {
        guard_required(
            self.required_refs().collect(),
            refs.iter()
                .map(|x| refs::scoped(&x.remote_id, &self.remote_id, x.name.as_bstr()))
                .collect(),
        )
    }
}
