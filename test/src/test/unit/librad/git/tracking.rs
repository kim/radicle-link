// Copyright © 2019-2020 The Radicle Foundation <hello@radicle.foundation>
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::collections::BTreeSet;

use git_ref_format::{lit, name, Namespaced, Qualified};
use it_helpers::git::create_commit;
use librad::{
    git::{
        storage::{ReadOnlyStorage as _, Storage},
        tracking::{
            is_tracked,
            migration,
            policy,
            track,
            tracked_peers,
            untrack,
            v1,
            Config,
            UntrackArgs,
        },
        Urn,
    },
    paths::Paths,
    reflike,
    PeerId,
    SecretKey,
};

#[test]
fn track_is_tracked() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let paths = Paths::from_root(&tmp).unwrap();
        let storage = Storage::open(&paths, SecretKey::new()).unwrap();
        let remote_peer = PeerId::from(SecretKey::new());
        let urn = Urn::new(git2::Oid::zero().into());

        assert!(track(
            &storage,
            &urn,
            Some(remote_peer),
            Config::default(),
            policy::Track::Any,
        )
        .unwrap()
        .is_ok());
        assert!(is_tracked(&storage, &urn, Some(remote_peer)).unwrap())
    }
}

#[test]
fn track_untrack_is_not_tracked() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let paths = Paths::from_root(&tmp).unwrap();
        let storage = Storage::open(&paths, SecretKey::new()).unwrap();
        let remote_peer = PeerId::from(SecretKey::new());
        let urn = Urn::new(git2::Oid::zero().into());

        assert!(track(
            &storage,
            &urn,
            Some(remote_peer),
            Config::default(),
            policy::Track::Any,
        )
        .unwrap()
        .is_ok());
        assert!(is_tracked(&storage, &urn, Some(remote_peer)).unwrap());
        assert!(untrack(&storage, &urn, remote_peer, UntrackArgs::default())
            .unwrap()
            .is_ok());
        assert!(!is_tracked(&storage, &urn, Some(remote_peer)).unwrap())
    }
}

#[test]
fn track_track_is_tracked() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let paths = Paths::from_root(&tmp).unwrap();
        let storage = Storage::open(&paths, SecretKey::new()).unwrap();
        let remote_peer = PeerId::from(SecretKey::new());
        let urn = Urn::new(git2::Oid::zero().into());

        assert!(track(
            &storage,
            &urn,
            Some(remote_peer),
            Config::default(),
            policy::Track::Any,
        )
        .unwrap()
        .is_ok());
        assert!(is_tracked(&storage, &urn, Some(remote_peer)).unwrap());
        assert!(track(
            &storage,
            &urn,
            Some(remote_peer),
            Config::default(),
            policy::Track::Any,
        )
        .unwrap()
        .is_ok());
        assert!(is_tracked(&storage, &urn, Some(remote_peer)).unwrap())
    }
}

#[test]
fn untrack_nonexistent_is_not_tracked() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let paths = Paths::from_root(&tmp).unwrap();
        let storage = Storage::open(&paths, SecretKey::new()).unwrap();
        let remote_peer = PeerId::from(SecretKey::new());
        let urn = Urn::new(git2::Oid::zero().into());

        assert!(untrack(&storage, &urn, remote_peer, UntrackArgs::default())
            .unwrap()
            .is_err());
        assert!(!is_tracked(&storage, &urn, Some(remote_peer)).unwrap());
    }
}

#[test]
fn track_yields_tracked() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let paths = Paths::from_root(&tmp).unwrap();
        let storage = Storage::open(&paths, SecretKey::new()).unwrap();
        let peer1 = PeerId::from(SecretKey::new());
        let peer2 = PeerId::from(SecretKey::new());
        let urn = Urn::new(git2::Oid::zero().into());

        assert!(
            track(&storage, &urn, None, Config::default(), policy::Track::Any,)
                .unwrap()
                .is_ok()
        );
        assert!(track(
            &storage,
            &urn,
            Some(peer1),
            Config::default(),
            policy::Track::Any,
        )
        .unwrap()
        .is_ok());
        assert!(track(
            &storage,
            &urn,
            Some(peer2),
            Config::default(),
            policy::Track::Any,
        )
        .unwrap()
        .is_ok());
        assert_eq!(
            [peer1, peer2].iter().copied().collect::<BTreeSet<_>>(),
            tracked_peers(&storage, Some(&urn))
                .unwrap()
                .collect::<Result<BTreeSet<_>, _>>()
                .unwrap()
        )
    }
}

#[test]
fn tracked_ignores_urn_path() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let paths = Paths::from_root(&tmp).unwrap();
        let storage = Storage::open(&paths, SecretKey::new()).unwrap();
        let remote_peer = PeerId::from(SecretKey::new());
        let urn = Urn::new(git2::Oid::zero().into());

        assert!(track(
            &storage,
            &urn,
            Some(remote_peer),
            Config::default(),
            policy::Track::Any,
        )
        .unwrap()
        .is_ok());

        let urn = urn.with_path(reflike!("ri/ra/rutsch"));
        assert_eq!(
            vec![remote_peer],
            tracked_peers(&storage, Some(&urn))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        )
    }
}

#[test]
fn untrack_with_prune() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let paths = Paths::from_root(&tmp).unwrap();
        let storage = Storage::open(&paths, SecretKey::new()).unwrap();
        let remote_peer = PeerId::from(SecretKey::new());
        let urn = Urn::new(git2::Oid::zero().into());

        assert!(track(
            &storage,
            &urn,
            Some(remote_peer),
            Config::default(),
            policy::Track::Any,
        )
        .unwrap()
        .is_ok());

        let branch = Namespaced::from(lit::refs_namespaces(
            &urn,
            Qualified::from(lit::refs_remotes(name::Component::from(&remote_peer)))
                .join(name::HEADS)
                .join(name::MAIN),
        ));

        {
            let repo = git2::Repository::open(paths.git_dir()).unwrap();
            create_commit(&repo, branch.clone().into_qualified()).unwrap();
        }

        untrack(
            &storage,
            &urn,
            remote_peer,
            UntrackArgs::prune(policy::Untrack::Any),
        )
        .unwrap()
        .unwrap();

        assert!(storage
            .reference(&branch.into_qualified().into_refstring())
            .unwrap()
            .is_none())
    }
}

#[test]
fn untrack_no_prune() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let paths = Paths::from_root(&tmp).unwrap();
        let storage = Storage::open(&paths, SecretKey::new()).unwrap();
        let remote_peer = PeerId::from(SecretKey::new());
        let urn = Urn::new(git2::Oid::zero().into());

        assert!(track(
            &storage,
            &urn,
            Some(remote_peer),
            Config::default(),
            policy::Track::Any,
        )
        .unwrap()
        .is_ok());

        let branch = Namespaced::from(lit::refs_namespaces(
            &urn,
            Qualified::from(lit::refs_remotes(name::Component::from(&remote_peer)))
                .join(name::HEADS)
                .join(name::MAIN),
        ));

        {
            let repo = git2::Repository::open(paths.git_dir()).unwrap();
            create_commit(&repo, branch.clone().into_qualified()).unwrap();
        }

        untrack(
            &storage,
            &urn,
            remote_peer,
            UntrackArgs::new(policy::Untrack::Any),
        )
        .unwrap()
        .unwrap();

        assert!(storage
            .reference(&branch.into_qualified().into_refstring())
            .unwrap()
            .is_some())
    }
}

#[test]
fn migration() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let paths = Paths::from_root(&tmp).unwrap();
        let storage = Storage::open(&paths, SecretKey::new()).unwrap();
        let peer1 = PeerId::from(SecretKey::new());
        let peer2 = PeerId::from(SecretKey::new());
        let urn = Urn::new(git2::Oid::zero().into());

        let branch = Namespaced::from(lit::refs_namespaces(
            &urn,
            Qualified::from(lit::refs_remotes(name::Component::from(&peer1)))
                .join(name::HEADS)
                .join(name::MAIN),
        ));

        // write a reference to the repository
        {
            let repo = git2::Repository::open(paths.git_dir()).unwrap();
            create_commit(&repo, branch.clone().into_qualified()).unwrap();
        }

        assert!(v1::track(&storage, &urn, peer1,).unwrap());
        assert!(v1::track(&storage, &urn, peer2,).unwrap());

        migration::migrate(&storage, [urn.clone()]).unwrap();
        assert_eq!(
            BTreeSet::from([peer1, peer2]),
            tracked_peers(&storage, Some(&urn))
                .unwrap()
                .collect::<Result<BTreeSet<_>, _>>()
                .unwrap()
        );
        assert!(v1::tracked(&storage, &urn).unwrap().next().is_none());
        assert!(storage
            .reference(&branch.into_qualified().into_refstring())
            .unwrap()
            .is_some())
    }
}
