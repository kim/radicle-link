// Copyright © 2019-2020 The Radicle Foundation <hello@radicle.foundation>
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{
    convert::TryFrom as _,
    mem,
    net::SocketAddr,
    ops::Try,
    panic,
    time::{Duration, Instant},
};

use futures::stream::FuturesUnordered;
use itertools::Itertools as _;

use crate::{
    bloom,
    git::{
        identities,
        replication,
        storage::{self, Storage},
    },
    identities::SomeUrn,
    PeerId,
};

pub mod error;
pub mod rpc;
pub use rpc::{Ask, Offer};

pub const MAX_OFFER_TOTAL: usize = 10_000;

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub sync_period: Duration,
    pub bloom_filter_accuracy: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sync_period: Duration::from_secs(5 * 60),
            bloom_filter_accuracy: 0.0001,
        }
    }
}

pub struct State {
    config: Config,
    deadline: Instant,
    snapshot: Option<bloom::BloomFilter<SomeUrn>>,
}

impl State {
    pub fn new(storage: &Storage, config: Config) -> Result<Self, error::State> {
        let snapshot = identities::any::bloom(storage, config.bloom_filter_accuracy)?;
        let deadline = Instant::now() + config.sync_period;
        Ok(Self {
            config,
            deadline,
            snapshot,
        })
    }

    pub fn reset(&mut self, storage: &Storage) -> Result<(), error::State> {
        self.snapshot = identities::any::bloom(storage, self.config.bloom_filter_accuracy)?;
        self.deadline = Instant::now() + self.config.sync_period;

        Ok(())
    }

    pub fn should_sync(&self) -> bool {
        self.snapshot.is_some() && Instant::now() > self.deadline
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn snapshot(&self) -> Option<&bloom::BloomFilter<SomeUrn>> {
        self.snapshot.as_ref()
    }
}

#[tracing::instrument(skip(storage), err)]
pub fn ask(
    storage: &Storage,
    request: Ask,
) -> Result<impl Iterator<Item = Result<Offer, error::Ask>> + '_, error::Ask> {
    let bloom = request
        .map(bloom::BloomFilter::try_from)
        .transpose()
        .map_err(error::Ask::Bloom)?;
    let offers = self::offers(storage, bloom)?.map(|of| of.map_err(error::Ask::from));

    Ok(offers)
}

#[tracing::instrument(skip(storage))]
pub fn on_offer<S>(
    storage: &S,
    offer: Offer,
    remote_id: PeerId,
    remote_addr: Option<SocketAddr>,
) -> impl futures::Stream<Item = Result<SomeUrn, error::Offer>> + '_
where
    S: storage::Pooled + Send + Sync + 'static,
{
    offer
        .into_iter()
        .map(move |urn| async move {
            let SomeUrn::Git(gurn) = urn.clone();
            let storage = storage.get().await?;
            let task = tokio::task::spawn_blocking(move || {
                replication::replicate(storage.as_ref(), None, gurn, remote_id, remote_addr)
            });

            match task.await {
                Err(e) => {
                    if let Ok(panik) = e.try_into_panic() {
                        panic::resume_unwind(panik)
                    } else {
                        Err(error::Offer::Cancelled)
                    }
                },

                Ok(res) => Ok(res.map(|()| urn)?),
            }
        })
        .collect::<FuturesUnordered<_>>()
}

fn offers(
    storage: &Storage,
    filter: Option<bloom::BloomFilter<SomeUrn>>,
) -> Result<impl Iterator<Item = Result<rpc::Offer, error::Ask>> + '_, error::Ask> {
    let offers = identities::any::list_urns(storage)?
        .map(|x| x.map_err(error::Ask::from))
        .filter_map_ok(move |urn| {
            let urn = SomeUrn::Git(urn);
            match filter.as_ref() {
                None => Some(urn),
                Some(bloom) => bloom.contains(&urn).then_some(urn),
            }
        })
        .try_chunked(rpc::MAX_OFFER_BATCH_SIZE)
        .map_ok(|chunk| rpc::Offer::try_from(chunk).expect("chunk size == batch size. qed"));

    Ok(offers)
}

// FIXME: We can't have a non-allocating chunker, because we can't put the bytes
// on the wire "zero-copy".
trait TryChunkedExt
where
    Self: Iterator + Sized,
    <Self as Iterator>::Item: Try,
{
    fn try_chunked(self, sz: usize) -> TryChunked<Self> {
        TryChunked {
            inner: self,
            sz,
            buf: Vec::with_capacity(sz),
        }
    }
}
impl<T> TryChunkedExt for T
where
    T: Iterator,
    <T as Iterator>::Item: Try,
{
}

#[must_use]
struct TryChunked<I>
where
    I: Iterator,
    I::Item: Try,
{
    inner: I,
    sz: usize,
    buf: Vec<<<I as Iterator>::Item as Try>::Ok>,
}

impl<I> Iterator for TryChunked<I>
where
    I: Iterator,
    I::Item: Try,
{
    type Item =
        Result<Vec<<<I as Iterator>::Item as Try>::Ok>, <<I as Iterator>::Item as Try>::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(i) = self.inner.next() {
            match i.into_result() {
                Err(e) => return Some(Err(e)),
                Ok(it) => {
                    self.buf.push(it);
                    if self.buf.len() == self.sz {
                        let chunk = mem::replace(&mut self.buf, Vec::with_capacity(self.sz));
                        return Some(Ok(chunk));
                    }
                },
            }
        }

        if !self.buf.is_empty() {
            Some(Ok(mem::take(&mut self.buf)))
        } else {
            None
        }
    }
}
