// Copyright © 2021 The Radicle Link Contributors
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use git_ref_format::{Qualified, RefStr, RefString};
use link_git::protocol::{oid, ObjectId};

use crate::refs;

mod mem;
pub use mem::Mem;

pub trait Refdb {
    type Oid: AsRef<oid> + Into<ObjectId>;

    type FindError: std::error::Error + Send + Sync + 'static;
    type TxError: std::error::Error + Send + Sync + 'static;
    type ReloadError: std::error::Error + Send + Sync + 'static;

    /// Peel `refname` to the first [`ObjectId`], or `None` if the ref does not
    /// exist.
    ///
    /// `refname` is to be interpreted as relative to the current namespace.
    fn refname_to_id<'a, Q>(&self, refname: Q) -> Result<Option<Self::Oid>, Self::FindError>
    where
        Q: AsRef<Qualified<'a>>;

    /// Apply the provided ref updates.
    ///
    /// This should be a transaction: either all updates (modulo the ones
    /// rejected by [`Policy`]) are applied, or none.
    ///
    /// On success, return the actually applied updates. That is, if an update
    /// has a [`Policy::Reject`], and was inded rejected, it is not included
    /// in the result.
    ///
    /// Note that refnames in [`Update`]s are to be interpreted as relative to
    /// the current namespace, _unless_ they are of type [`refs::Namespaced`].
    fn update<'a, I>(&mut self, updates: I) -> Result<Applied<'a>, Self::TxError>
    where
        I: IntoIterator<Item = Update<'a>>;

    /// Ensure on-disk state is considered.
    fn reload(&mut self) -> Result<(), Self::ReloadError>;
}

pub trait RefScan {
    type Oid: AsRef<oid> + Into<ObjectId>;
    type Scan: Iterator<Item = Result<(Qualified<'static>, Self::Oid), Self::Error>>;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Traverse all refs in the current namespace matching `prefix`.
    fn scan<O, P>(self, prefix: O) -> Result<Self::Scan, Self::Error>
    where
        O: Into<Option<P>>,
        P: AsRef<str>;
}

#[derive(Clone, Debug)]
pub enum Update<'a> {
    Direct {
        name: refs::Qualified<'a>,
        target: ObjectId,

        /// Policy to apply when an [`Update`] would not apply as a
        /// fast-forward.
        ///
        /// An update is a fast-forward iff:
        ///
        /// 1. A ref with the same name already exists
        /// 2. The ref is a direct ref, and the update is a [`Update::Direct`]
        /// 3. Both the existing and the update [`ObjectId`] point to a commit
        ///    object without peeling
        /// 4. The existing commit is an ancestor of the update commit
        ///
        /// or:
        ///
        /// 1. A ref with the same name does not already exist
        no_ff: Policy,
    },
    Symbolic {
        name: refs::Qualified<'a>,
        target: SymrefTarget<'a>,

        /// Policy to apply when the ref already exists, but is a direct ref
        /// before the update.
        type_change: Policy,
    },
}

impl Update<'_> {
    pub fn refname(&self) -> &RefStr {
        match self {
            Self::Direct { name, .. } => name,
            Self::Symbolic { name, .. } => name,
        }
    }

    pub fn into_owned<'b>(self) -> Update<'b> {
        match self {
            Self::Direct {
                name,
                target,
                no_ff,
            } => Update::Direct {
                name: name.into_owned(),
                target,
                no_ff,
            },

            Self::Symbolic {
                name,
                target,
                type_change,
            } => Update::Symbolic {
                name: name.into_owned(),
                target: target.into_owned(),
                type_change,
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Policy {
    /// Abort the entire transaction.
    Abort,
    /// Reject this update, but continue the transaction.
    Reject,
    /// Allow the update.
    Allow,
}

#[derive(Clone, Debug)]
pub struct SymrefTarget<'a> {
    pub name: refs::Namespaced<'a>,
    pub target: ObjectId,
}

impl SymrefTarget<'_> {
    pub fn name(&self) -> &RefStr {
        self.name.as_ref()
    }

    pub fn into_owned<'b>(self) -> SymrefTarget<'b> {
        SymrefTarget {
            name: self.name.into_owned(),
            target: self.target,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Updated {
    Direct { name: RefString, target: ObjectId },
    Symbolic { name: RefString, target: RefString },
}

#[derive(Debug, Default)]
pub struct Applied<'a> {
    pub rejected: Vec<Update<'a>>,
    pub updated: Vec<Updated>,
}

impl Applied<'_> {
    pub fn append(&mut self, other: &mut Self) {
        self.rejected.append(&mut other.rejected);
        self.updated.append(&mut other.updated);
    }

    pub fn into_owned<'b>(self) -> Applied<'b> {
        Applied {
            rejected: self.rejected.into_iter().map(Update::into_owned).collect(),
            updated: self.updated,
        }
    }
}
