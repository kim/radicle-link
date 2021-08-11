// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use crate::{error, ids, FetchState, FilteredRef, Identities, Update};

pub(crate) trait UpdateTips<T = Self> {
    fn prepare<'a, U, I>(
        &self,
        s: &FetchState<U>,
        ids: &I,
        refs: &'a [FilteredRef<T>],
    ) -> Result<Vec<Update<'a>>, error::Prepare<I::VerificationError>>
    where
        U: ids::Urn + Ord,
        I: Identities<Urn = U>;
}

pub(crate) trait Layout<T = Self> {
    /// Validate that all advertised refs conform to an expected layout.
    ///
    /// The supplied `refs` are `ls-ref`-advertised refs filtered through
    /// [`crate::Negotiation::ref_filter`].
    fn pre_validate(&self, refs: &[FilteredRef<T>]) -> Result<(), error::Layout>;
}
