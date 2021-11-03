// Copyright © 2019-2020 The Radicle Foundation <hello@radicle.foundation>
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{net::SocketAddr, ops::Deref};

use futures::TryFutureExt as _;
use librad::{
    git::{
        identities::{self, Person, Project},
        storage::Storage,
    },
    identities::{
        delegation::{self, Direct},
        payload,
    },
    net::{connection::LocalInfo, peer::Peer, replication},
    Signer,
};
use tracing::{info, instrument};

pub struct TestPerson {
    pub owner: Person,
}

impl TestPerson {
    pub fn create(storage: &Storage) -> anyhow::Result<Self> {
        let peer_id = storage.peer_id();
        let alice = identities::person::create(
            storage,
            payload::Person {
                name: "alice".into(),
            },
            Direct::new(*peer_id.as_public_key()),
        )?;

        Ok(Self { owner: alice })
    }

    pub fn update(self, storage: &Storage) -> anyhow::Result<Self> {
        let payload = payload::Person {
            name: "alice-laptop".into(),
        }
        .into();
        let owner =
            identities::person::update(storage, &self.owner.urn(), None, Some(payload), None)?;
        Ok(Self { owner })
    }

    /// Pull (fetch or clone) the project from known running peer `A` to peer
    /// `B`.
    #[instrument(name = "test_person", skip(self, from, to), err)]
    pub async fn pull<A, B, S>(&self, from: &A, to: &B) -> anyhow::Result<replication::Success>
    where
        A: Deref<Target = Peer<S>> + LocalInfo<Addr = SocketAddr>,
        B: Deref<Target = Peer<S>>,

        S: Signer + Clone,
    {
        let remote_peer = from.local_peer_id();
        let remote_addrs = from.listen_addrs();
        let urn = self.owner.urn();

        info!("pull from {} to {}", remote_peer, to.peer_id());

        let res = to
            .replicate((remote_peer, remote_addrs), urn, None)
            .err_into::<replication::ErrorBox>()
            .await?;

        Ok(res)
    }
}

pub struct TestProject {
    pub owner: Person,
    pub project: Project,
}

impl TestProject {
    pub fn create(storage: &Storage) -> anyhow::Result<Self> {
        let peer_id = storage.peer_id();
        let alice = identities::person::create(
            storage,
            payload::Person {
                name: "alice".into(),
            },
            Direct::new(*peer_id.as_public_key()),
        )?;
        let local_id = identities::local::load(storage, alice.urn())?
            .expect("local id must exist as we just created it");
        let proj = identities::project::create(
            storage,
            local_id,
            radicle_link(),
            delegation::Indirect::from(alice.clone()),
        )?;

        Ok(Self {
            owner: alice,
            project: proj,
        })
    }

    pub fn from_test_person(storage: &Storage, person: TestPerson) -> anyhow::Result<Self> {
        let local_id = identities::local::load(storage, person.owner.urn())?
            .expect("local id must exist as we just created it");
        let proj = identities::project::create(
            storage,
            local_id,
            radicle_link(),
            delegation::Indirect::from(person.owner.clone()),
        )?;

        Ok(Self {
            owner: person.owner,
            project: proj,
        })
    }

    pub fn from_project_payload(
        storage: &Storage,
        owner: Person,
        payload: payload::Project,
    ) -> anyhow::Result<Self> {
        let local_id = identities::local::load(storage, owner.urn())?
            .expect("local id must exist as we just created it");
        let proj = identities::project::create(
            storage,
            local_id,
            payload,
            delegation::Indirect::from(owner.clone()),
        )?;

        Ok(Self {
            owner,
            project: proj,
        })
    }

    /// Pull (fetch or clone) the project from known running peer `A` to peer
    /// `B`.
    #[instrument(name = "test_project", skip(self, from, to))]
    pub async fn pull<A, B, S>(&self, from: &A, to: &B) -> anyhow::Result<replication::Success>
    where
        A: Deref<Target = Peer<S>> + LocalInfo<Addr = SocketAddr>,
        B: Deref<Target = Peer<S>>,

        S: Signer + Clone,
    {
        let remote_peer = from.local_peer_id();
        let remote_addrs = from.listen_addrs();
        let urn = self.project.urn();

        info!("pull from {} to {}", remote_peer, to.peer_id());

        let res = to
            .replicate((remote_peer, remote_addrs), urn, None)
            .err_into::<replication::ErrorBox>()
            .await?;

        Ok(res)
    }
}

pub fn create_test_project(storage: &Storage) -> Result<TestProject, anyhow::Error> {
    TestProject::create(storage)
}

pub fn radicle_link() -> payload::Project {
    payload::Project {
        name: "radicle-link".into(),
        description: Some("pea two pea".into()),
        default_branch: Some("next".into()),
    }
}
