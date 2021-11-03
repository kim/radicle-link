// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{
    borrow::Cow,
    collections::{BTreeSet, HashSet},
};

use bstr::{BStr, BString, ByteSlice as _, ByteVec as _};
use link_crypto::PeerId;
use link_git_protocol::{oid, Ref};

use crate::{
    error,
    internal::{Layout, UpdateTips},
    refs,
    sigrefs,
    FetchState,
    FilteredRef,
    Identities,
    Negotiation,
    Policy,
    Refdb,
    Update,
    WantsHaves,
};

#[derive(Debug)]
pub struct Fetch<Oid> {
    /// The local id.
    pub local_id: PeerId,
    /// The peer being fetched from.
    pub remote_id: PeerId,
    /// The stack of signed refs describing which refs we'll ask for.
    pub signed_refs: sigrefs::Combined<Oid>,
}

impl<T> Fetch<T> {
    fn scoped<'a, 'b: 'a>(
        &self,
        id: &'a PeerId,
        name: impl Into<Cow<'b, BStr>>,
    ) -> refs::Scoped<'a, 'b> {
        refs::scoped(id, &self.remote_id, name)
    }

    fn signed(&self, id: &PeerId, refname: impl AsRef<BStr>) -> Option<&T> {
        self.signed_refs
            .refs
            .get(id)
            .and_then(|refs| refs.refs.get(refname.as_ref()))
    }

    fn is_signed(&self, id: &PeerId, refname: impl AsRef<BStr>) -> bool {
        self.signed(id, refname).is_some()
    }

    fn is_tracked(&self, id: &PeerId) -> bool {
        self.signed_refs.remotes.contains(id)
    }
}

impl<T: AsRef<oid>> Negotiation for Fetch<T> {
    fn ref_prefixes(&self) -> Vec<refs::Scoped<'_, '_>> {
        let remotes = self
            .signed_refs
            .remotes
            .iter()
            .filter(move |id| *id != &self.local_id)
            .flat_map(move |id| {
                vec![
                    self.scoped(id, refs::Prefix::Heads),
                    self.scoped(id, refs::Prefix::Notes),
                    self.scoped(id, refs::Prefix::Tags),
                ]
            });
        let signed = self
            .signed_refs
            .refs
            .iter()
            .filter(move |(id, _)| *id != &self.local_id)
            .flat_map(move |(id, refs)| {
                refs.refs
                    .iter()
                    .map(move |(name, _)| self.scoped(id, name.as_bstr()))
            });

        remotes.chain(signed).collect()
    }

    fn ref_filter(&self, r: Ref) -> Option<FilteredRef<Self>> {
        use either::Either::*;
        use refs::parsed::{Cat, Identity, Refs};

        let (refname, tip) = refs::into_unpacked(r);
        let parsed = refs::parse::<Identity>(refname.as_bstr())?;
        match &parsed.inner {
            Right(Refs { cat, name, .. }) => match cat {
                // Only known "standard" refs.
                //
                // Peeking should've already gotten us the "rad" refs, and by
                // ignoring them here we don't have to worry about the remote
                // end becoming inconsistent between peek and fetch.
                //
                // XXX: allow to configure fetching "strange" refs
                Cat::Unknown(_) => {
                    warn!("skipping unknown cat {}", cat);
                    None
                },
                Cat::Heads | Cat::Notes | Cat::Tags => {
                    let refname_no_remote = {
                        let mut x = BString::from(refs::component::REFS);
                        x.push_byte(refs::SEPARATOR);
                        x.push_str(cat.as_bytes());
                        x.push_byte(refs::SEPARATOR);
                        for (i, y) in name.iter().enumerate() {
                            if i > 0 {
                                x.push_byte(refs::SEPARATOR);
                            }
                            x.push_str(y);
                        }
                        x
                    };
                    let remote_id = *parsed.remote.as_ref().unwrap_or(&self.remote_id);
                    if self.is_tracked(&remote_id) || self.is_signed(&remote_id, &refname_no_remote)
                    {
                        Some(FilteredRef::new(refname, tip, &remote_id, parsed))
                    } else {
                        warn!(
                            %refname_no_remote,
                            "skipping {} as it is neither signed nor tracked", refname
                        );
                        None
                    }
                },
            },

            Left(_) => None,
        }
    }

    fn wants_haves<'a, R: Refdb>(
        &self,
        db: &R,
        refs: impl IntoIterator<Item = FilteredRef<Self>>,
    ) -> Result<WantsHaves<Self>, R::FindError> {
        let mut wanted = HashSet::new();
        let mut wants = BTreeSet::new();
        let mut haves = BTreeSet::new();

        for r in refs {
            let refname = refs::remote_tracking(&r.remote_id, r.name.as_bstr());
            let refname_no_remote = refs::owned(r.name.as_bstr());

            let have = db.refname_to_id(&refname)?;
            if let Some(oid) = have.as_ref() {
                haves.insert(oid.as_ref().to_owned());
            }

            // If we have a signed ref, we `want` the signed oid. Otherwise, and
            // if the remote id is in the tracking graph, we `want` what we got
            // offered.
            let want: Option<&oid> = self
                .signed(&r.remote_id, &refname_no_remote)
                .map(|s| s.as_ref())
                .or_else(|| self.is_tracked(&r.remote_id).then_some(&r.tip));

            match (want, have) {
                (Some(want), Some(have)) if want == have.as_ref() => {
                    // No need to want what we already have
                },
                (None, _) => {
                    // Unsolicited
                },
                (Some(_want), _) => {
                    wants.insert(r.tip);
                    wanted.insert(r);
                },
            }
        }

        Ok(WantsHaves {
            wanted,
            wants,
            haves,
        })
    }
}

impl<T: AsRef<oid>> UpdateTips for Fetch<T> {
    fn prepare<'a, U, I: Identities>(
        &self,
        _: &FetchState<U>,
        _: &I,
        refs: &'a [FilteredRef<Self>],
    ) -> Result<Vec<Update<'a>>, error::Prepare<I::VerificationError>> {
        let mut updates = Vec::new();
        for r in refs {
            debug_assert!(r.remote_id != self.local_id, "never touch our own");
            let refname = refs::remote_tracking(&r.remote_id, r.name.as_bstr());
            updates.push(Update::Direct {
                name: Cow::from(refname),
                target: r.tip,
                no_ff: Policy::Allow,
            });
        }

        Ok(updates)
    }
}

impl<T> Layout for Fetch<T> {
    // [`Fetch`] may request only a part of the refs tree, so no layout error
    // can be determined from the advertised refs alone.
    //
    // XXX: We could reject if only a subset of the signed refs are present. This
    // would interact with fetchspecs, so requires runtime configuration.
    fn pre_validate(&self, _: &[FilteredRef<Self>]) -> Result<(), error::Layout> {
        Ok(())
    }
}
