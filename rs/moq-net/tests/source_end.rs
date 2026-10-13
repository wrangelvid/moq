//! A track that ends because its source ended reports the source's own error, across a
//! relay too. `Dropped` means only that a handle was dropped without an end, so neither
//! it nor its wire form, `internal error`, may reach a consumer whose source ended
//! properly. A path that lost its route ends `Unroutable`: the subscriber's own session
//! is fine.

mod support;

use std::time::Duration;

use moq_net::{Error, Hop, SessionError, StreamError, Timestamp, Version};
use support::harness::{MockConnectOptions, MockPair, connect_mock};

const TIMEOUT: Duration = Duration::from_secs(10);

/// The moq-lite versions, which carry every stream code a relay forwards.
const LITE: &[&str] = &["moq-lite-03", "moq-lite-05", "moq-lite-06", "moq-lite-07-wip"];

/// A millisecond track: the frames written here carry timestamps.
fn timed() -> moq_net::track::Info {
	moq_net::track::Info::default().with_timescale(moq_net::Timescale::MILLI)
}

fn produce_origin(hop: u64) -> moq_net::origin::Producer {
	let (producer, driver) = moq_net::origin::Producer::new(moq_net::origin::Config::new(Hop::new(hop).unwrap()));
	support::harness::spawn(driver);
	producer
}

/// A publisher, a relay, and a subscriber, each its own origin, with one mock session
/// on each hop.
struct Chain {
	publisher: moq_net::origin::Producer,
	subscriber: moq_net::origin::Producer,
	upstream: MockPair,
	downstream: MockPair,
	_relay: moq_net::origin::Producer,
}

impl Chain {
	async fn new(upstream: &str, downstream: &str, latency: Duration) -> Self {
		let publisher = produce_origin(1);
		let relay = produce_origin(2);
		let subscriber = produce_origin(3);

		let mut options = MockConnectOptions::new(upstream.parse::<Version>().unwrap());
		options.server_publish = Some(publisher.consume());
		options.client_subscribe = Some(relay.clone());
		options.latency = latency;
		let upstream = connect_mock(options).await;

		let mut options = MockConnectOptions::new(downstream.parse::<Version>().unwrap());
		options.server_publish = Some(relay.consume());
		options.client_subscribe = Some(subscriber.clone());
		options.latency = latency;
		let downstream = connect_mock(options).await;

		Self {
			publisher,
			subscriber,
			upstream,
			downstream,
			_relay: relay,
		}
	}

	/// The subscriber's handle on `path`, once the relay announces it.
	async fn resolve(&self, path: &str) -> moq_net::broadcast::Consumer {
		moq_net_sim::timeout(TIMEOUT, self.subscriber.consume().routed_broadcast(path))
			.await
			.expect("resolve timeout")
			.expect("broadcast resolves")
	}
}

/// Whether `err` says the path has no route, as raised here or as a peer reset it.
fn unroutable(err: &Error) -> bool {
	matches!(err, Error::Unroutable | Error::Stream(StreamError::Unroutable))
}

/// Subscribe to `name` and read its first group, so the relay's copy has delivered.
async fn subscribe_delivered(
	remote: &moq_net::broadcast::Consumer,
	track: &mut moq_net::track::Producer,
	name: &str,
) -> moq_net::track::Subscriber {
	let mut sub = moq_net_sim::timeout(TIMEOUT, remote.track(name).unwrap().subscribe(None))
		.await
		.expect("subscribe timeout")
		.expect("subscribe");
	let mut group = track.append_group().unwrap();
	group.write_frame(Timestamp::ZERO, b"first".as_ref()).unwrap();
	group.finish().unwrap();
	let mut group = moq_net_sim::timeout(TIMEOUT, sub.recv_group())
		.await
		.expect("first group timeout")
		.expect("first group")
		.expect("the track ended before its first group");
	assert_eq!(group.read_frame().await.unwrap().unwrap().payload, b"first".as_ref());
	sub
}

/// The publisher closes its broadcast under an active subscription and ends the track,
/// in either order: the subscriber two hops away reads the track's own end, its error
/// or a clean finish. An epoch lets a relay re-splice a failed copy, which must not
/// replace that error either.
#[moq_net_sim::test]
async fn closing_a_broadcast_relays_its_tracks_own_end() {
	for version in LITE {
		for epoch in [false, true] {
			for abort in [Some(Error::App(42)), None] {
				for end_first in [true, false] {
					let what = format!("{version} epoch={epoch} abort={abort:?} end_first={end_first}");
					let chain = Chain::new(version, version, Duration::ZERO).await;
					let broadcast = chain.publisher.create_broadcast("bcast").unwrap();
					let mut track = broadcast.create_track("video", timed()).unwrap();
					let route = match epoch {
						true => moq_net::origin::Route::default().with_epoch(moq_net::Epoch::mint()),
						false => moq_net::origin::Route::default(),
					};
					broadcast.announce(route).unwrap();

					let remote = chain.resolve("bcast").await;
					let mut sub = subscribe_delivered(&remote, &mut track, "video").await;

					let end = |track: &mut moq_net::track::Producer| match &abort {
						Some(err) => track.clone().abort(err.clone()).unwrap(),
						None => track.finish().unwrap(),
					};
					if end_first {
						end(&mut track);
						broadcast.close();
					} else {
						broadcast.close();
						// The retraction crosses both hops before the track ends.
						moq_net_sim::sleep(Duration::from_millis(1)).await;
						end(&mut track);
					}

					let res = moq_net_sim::timeout(TIMEOUT, sub.recv_group())
						.await
						.unwrap_or_else(|_| panic!("{what}: the track never ended"))
						.map(|group| group.map(|group| group.sequence));
					match (&abort, res) {
						(Some(_), Err(Error::Stream(StreamError::App(42)))) | (None, Ok(None)) => {}
						(_, res) => panic!("{what}: ended with {res:?}"),
					}
				}
			}
		}
	}
}

/// A revocation bridged from moq-lite reaches a moq-transport subscriber as
/// UNAUTHORIZED, not as an internal error.
#[moq_net_sim::test]
async fn a_bridged_revocation_reaches_ietf_as_unauthorized() {
	for version in ["moq-transport-14", "moq-transport-17", "moq-transport-22"] {
		let chain = Chain::new("moq-lite-06", version, Duration::ZERO).await;
		let broadcast = chain.publisher.create_broadcast("bcast").unwrap();
		let mut track = broadcast.create_track("video", timed()).unwrap();
		broadcast.announce(Default::default()).unwrap();

		let remote = chain.resolve("bcast").await;
		let mut sub = subscribe_delivered(&remote, &mut track, "video").await;

		track.clone().abort(Error::Unauthorized).unwrap();

		let err = moq_net_sim::timeout(TIMEOUT, sub.recv_group())
			.await
			.unwrap_or_else(|_| panic!("{version}: the track never ended"))
			.err()
			.unwrap_or_else(|| panic!("{version}: the track ended cleanly"));
		assert!(matches!(err, Error::Unauthorized), "{version}: ended with {err:?}");
	}
}

/// A subscribe sent while the route is being withdrawn, at every point the withdrawal
/// can be on either hop, ends `Unroutable`.
#[moq_net_sim::test]
async fn a_subscribe_against_a_withdrawn_route_is_unroutable() {
	const LATENCY: Duration = Duration::from_millis(5);
	for version in LITE {
		for offset in 0..=12 {
			let chain = Chain::new(version, version, LATENCY).await;
			let broadcast = chain.publisher.create_broadcast("bcast").unwrap();
			let _track = broadcast.create_track("video", timed()).unwrap();
			broadcast.announce(Default::default()).unwrap();
			let remote = chain.resolve("bcast").await;

			broadcast.close();
			moq_net_sim::sleep(Duration::from_millis(offset)).await;

			let res = match remote.track("video") {
				Ok(track) => moq_net_sim::timeout(TIMEOUT, async {
					let mut sub = track.subscribe(None).await?;
					sub.recv_group().await.map(|_| ())
				})
				.await
				.unwrap_or_else(|_| panic!("{version} +{offset}ms: the subscribe never ended")),
				Err(err) => Err(err),
			};
			match res {
				Err(err) => assert!(unroutable(&err), "{version} +{offset}ms: ended with {err:?}"),
				Ok(()) => panic!("{version} +{offset}ms: a withdrawn broadcast served a group"),
			}
		}
	}
}

/// How the publisher's session dies under a request the relay forwarded to it.
#[derive(Clone, Copy, Debug)]
enum Death {
	/// The publisher closes its session with a code.
	Close,
	/// The connection goes silent until the relay's idle timeout, with no code.
	Timeout,
}

/// A track info request, or a subscription, forwarded to a publisher session that is
/// dead but not yet timed out: when the relay gives up on that session, the subscriber
/// learns the path is unroutable, never an internal error.
#[moq_net_sim::test]
async fn a_request_to_a_dying_session_is_unroutable() {
	for version in LITE {
		for death in [Death::Close, Death::Timeout] {
			for subscribe in [false, true] {
				// Before the TRACK stream, track info never crosses the wire.
				if !subscribe && *version == "moq-lite-03" {
					continue;
				}
				let what = format!("{version} {death:?} subscribe={subscribe}");
				let chain = Chain::new(version, version, Duration::ZERO).await;
				let broadcast = chain.publisher.create_broadcast("bcast").unwrap();
				let _track = broadcast.create_track("catalog.json", timed()).unwrap();
				broadcast.announce(Default::default()).unwrap();
				let remote = chain.resolve("bcast").await;

				// Every request the relay forwards upstream from here on is lost on the way.
				chain.upstream.client_transport.hold_bidis();
				let track = remote.track("catalog.json").unwrap();
				let request = moq_net_sim::spawn(async move {
					match subscribe {
						true => track.subscribe(None).await?.recv_group().await.map(|_| ()),
						false => track.query().await.map(|_| ()),
					}
				});
				moq_net_sim::sleep(Duration::from_millis(1)).await;

				match death {
					Death::Close => chain.upstream.server.abort(Error::Session(SessionError::Internal)),
					Death::Timeout => chain.upstream.server_transport.time_out(),
				}

				let err = moq_net_sim::timeout(TIMEOUT, request)
					.await
					.unwrap_or_else(|_| panic!("{what}: the request never ended"))
					.expect("request panicked")
					.err()
					.unwrap_or_else(|| panic!("{what}: a dead session answered"));
				assert!(unroutable(&err), "{what}: ended with {err:?}");
				let resets = chain.downstream.server_transport.resets();
				assert!(
					!resets.contains(&StreamError::Internal.to_code()),
					"{what}: the relay reset with internal error: {resets:?}"
				);
			}
		}
	}
}
