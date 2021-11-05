// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::collections::BTreeMap;

use bstr::BStr;
use futures_lite::future::block_on;
use tracing::Instrument as _;

use crate::{
    error,
    ids,
    internal::{Layout, UpdateTips},
    oid,
    refdb,
    refs,
    Applied,
    Identities,
    LocalPeer,
    Negotiation,
    Net,
    ObjectId,
    PeerId,
    Refdb,
    SignedRefs,
    Sigrefs,
    SkippedFetch,
    Tracking,
    Update,
    Urn,
};

type IdentityTips = BTreeMap<PeerId, ObjectId>;
type DelegationTips<Urn> = BTreeMap<PeerId, BTreeMap<Urn, ObjectId>>;
type SigrefTips = BTreeMap<PeerId, ObjectId>;

pub(crate) struct FetchState<Urn> {
    refs: refdb::Mem,
    idts: IdentityTips,
    dels: DelegationTips<Urn>,
    sigs: SigrefTips,
    tips: Vec<Update<'static>>,
    trks: Vec<(PeerId, Option<Urn>)>,
}

impl<Urn> Default for FetchState<Urn> {
    fn default() -> Self {
        Self {
            refs: Default::default(),
            idts: Default::default(),
            dels: Default::default(),
            sigs: Default::default(),
            tips: Default::default(),
            trks: Default::default(),
        }
    }
}

impl<U> FetchState<U>
where
    U: ids::Urn + Ord,
{
    pub fn step<C, S>(
        &mut self,
        cx: &mut C,
        step: S,
    ) -> Result<(S, Option<SkippedFetch>), error::Error>
    where
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
                            self.insert_id_tip(r.remote_id, r.tip);
                        },

                        refs::parsed::Rad::Ids { urn } => {
                            if let Ok(urn) = C::Urn::try_from_id(urn) {
                                self.insert_delegation_tip(r.remote_id, urn, r.tip);
                            }
                        },

                        refs::parsed::Rad::SignedRefs => {
                            self.insert_sigref_tip(r.remote_id, r.tip);
                        },

                        _ => {},
                    }
                }
            }

            self.update_all(
                UpdateTips::prepare(&step, self, cx, refs)?
                    .into_iter()
                    .map(|u| u.into_owned()),
            );
        }

        Ok((step, res.err()))
    }
}

impl<Urn> FetchState<Urn>
where
    Urn: Ord,
{
    pub fn lookup_delegations<'a>(
        &'a self,
        remote: &PeerId,
    ) -> impl Fn(&Urn) -> Option<&'a ObjectId> {
        let ids = self.dels.get(remote);
        move |urn| ids.and_then(|x| x.get(urn))
    }

    pub fn id_tip(&self, of: &PeerId) -> Option<&ObjectId> {
        self.idts.get(of)
    }

    fn insert_id_tip(&mut self, of: PeerId, tip: ObjectId) {
        self.idts.insert(of, tip);
    }

    pub fn sigref_tip(&self, of: &PeerId) -> Option<&ObjectId> {
        self.sigs.get(of)
    }

    fn insert_sigref_tip(&mut self, of: PeerId, tip: ObjectId) {
        self.sigs.insert(of, tip);
    }

    fn insert_delegation_tip(&mut self, remote_id: PeerId, urn: Urn, tip: ObjectId) {
        self.dels
            .entry(remote_id)
            .or_insert_with(BTreeMap::new)
            .insert(urn, tip);
    }

    pub fn track(&mut self, peer: PeerId, urn: Option<Urn>) {
        self.trks.push((peer, urn));
    }

    pub fn track_all<I>(&mut self, other: I)
    where
        I: IntoIterator<Item = (PeerId, Option<Urn>)>,
    {
        self.trks.extend(other);
    }

    pub fn drain_trackings(&mut self) -> impl Iterator<Item = (PeerId, Option<Urn>)> + '_ {
        self.trks.drain(..)
    }

    pub fn update_all<'a, I>(&mut self, other: I) -> Applied<'a>
    where
        I: IntoIterator<Item = Update<'a>>,
    {
        let mut ap = Applied::default();
        for up in other {
            self.tips.push(up.clone().into_owned());
            ap.append(&mut self.refs.update(Some(up)).into_ok());
        }
        ap
    }

    pub fn drain_updates(&mut self) -> impl Iterator<Item = Update<'static>> + '_ {
        self.tips.drain(..)
    }

    pub fn as_shim<'a, T>(&'a mut self, of: &'a mut T) -> Shim<'a, T, Urn> {
        Shim {
            inner: of,
            fetch: self,
        }
    }
}

pub(crate) struct Shim<'a, T, U> {
    inner: &'a mut T,
    fetch: &'a mut FetchState<U>,
}

impl<T, U> Refdb for Shim<'_, T, U>
where
    T: Refdb,
    U: Ord,
{
    type Oid = <refdb::Mem as Refdb>::Oid;

    type Scan<'a> = <refdb::Mem as Refdb>::Scan<'a>;

    type FindError = <T as Refdb>::FindError;
    type ScanError = <refdb::Mem as Refdb>::ScanError;
    type TxError = <refdb::Mem as Refdb>::TxError;
    type ReloadError = <refdb::Mem as Refdb>::ReloadError;

    fn refname_to_id(
        &self,
        refname: impl AsRef<BStr>,
    ) -> Result<Option<Self::Oid>, Self::FindError> {
        let cached = self.fetch.refs.refname_to_id(&refname).into_ok();
        if cached.is_some() {
            Ok(cached)
        } else {
            self.inner
                .refname_to_id(refname)
                .map(|oid| oid.map(|oid| ObjectId::from(oid.as_ref())))
        }
    }

    fn scan<O, P>(&self, prefix: O) -> Result<Self::Scan<'_>, Self::ScanError>
    where
        O: Into<Option<P>>,
        P: AsRef<str>,
    {
        self.fetch.refs.scan(prefix)
    }

    fn update<'a, I>(&mut self, updates: I) -> Result<Applied<'a>, Self::TxError>
    where
        I: IntoIterator<Item = Update<'a>>,
    {
        Ok(self.fetch.update_all(updates))
    }

    fn reload(&mut self) -> Result<(), Self::ReloadError> {
        self.fetch.refs.reload()
    }
}

impl<T, U> SignedRefs for Shim<'_, T, U>
where
    T: SignedRefs,
    U: Ord,
{
    type Oid = T::Oid;
    type Error = T::Error;

    fn load(&self, of: &PeerId, cutoff: usize) -> Result<Option<Sigrefs<Self::Oid>>, Self::Error> {
        if self.fetch.sigs.is_empty() {
            SignedRefs::load(self.inner, of, cutoff)
        } else {
            match self.fetch.sigref_tip(of) {
                None => Ok(None),
                Some(tip) => SignedRefs::load_at(self.inner, *tip, of, cutoff),
            }
        }
    }

    fn load_at(
        &self,
        treeish: impl Into<ObjectId>,
        of: &PeerId,
        cutoff: usize,
    ) -> Result<Option<Sigrefs<Self::Oid>>, Self::Error> {
        self.inner.load_at(treeish, of, cutoff)
    }

    fn update(&self) -> Result<Option<Self::Oid>, Self::Error> {
        self.inner.update()
    }
}

impl<T, U> Tracking for Shim<'_, T, U>
where
    T: Tracking<Urn = U>,
    U: Urn + Clone + Ord,
{
    type Urn = U;

    type Tracked = T::Tracked;
    type Error = T::Error;

    fn track(&mut self, id: &PeerId, urn: Option<&Self::Urn>) -> Result<(), Self::Error> {
        self.fetch.track(*id, urn.cloned());
        Ok(())
    }

    fn tracked(&self, urn: Option<&Self::Urn>) -> Self::Tracked {
        self.inner.tracked(urn)
    }
}

impl<T, U> Identities for Shim<'_, T, U>
where
    T: Identities<Urn = U>,
    U: Urn,
{
    type Urn = U;
    type Oid = T::Oid;

    type VerifiedIdentity = T::VerifiedIdentity;
    type VerificationError = T::VerificationError;

    fn verify<H, F, V>(
        &self,
        head: H,
        resolve: F,
    ) -> Result<Self::VerifiedIdentity, Self::VerificationError>
    where
        H: AsRef<oid>,
        F: Fn(&Self::Urn) -> Option<V>,
        V: AsRef<oid>,
    {
        self.inner.verify(head, resolve)
    }

    fn newer(
        &self,
        a: Self::VerifiedIdentity,
        b: Self::VerifiedIdentity,
    ) -> Result<Self::VerifiedIdentity, error::IdentityHistory<Self::VerifiedIdentity>> {
        self.inner.newer(a, b)
    }
}

impl<T, U> LocalPeer for Shim<'_, T, U>
where
    T: LocalPeer,
{
    fn id(&self) -> &PeerId {
        self.inner.id()
    }
}
