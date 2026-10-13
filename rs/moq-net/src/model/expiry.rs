//! Wakes for group reads parked on their subscription's drift budget, and for groups held
//! across a route switch until the serving copy leaves them behind.
//!
//! A track's [`Wakes`] are driven by its writes: frames presenting and groups landing.
//! The cache pool's [`Wall`] drives the same reads on the wall clock, for a superseded
//! group whose successor arrived a full budget ago with nothing written since.

use std::collections::BTreeMap;
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::{Timestamp, cache};
use crate::time::Instant;

/// A track's parked group reads, indexed by the events that can expire them, so a frame
/// write or a group insert wakes only the reads whose verdict it could change.
///
/// A parked read goes stale once the live edge reaches its deadline: where its immediate
/// successor starts, plus its budget. The edge only ever takes a timestamp some frame
/// write presented, so the write that crosses a deadline wakes the reads behind it and
/// nobody else. The successor itself is watched per group (its first frame, its abort),
/// except for a group landing between the read and it, which reads park on by sequence.
///
/// A group held across a route switch parks in the serving copy's index the same way,
/// although that copy need not hold it; see `track::Consumer::poll_stale`.
///
/// Waking every parked read on every track change instead costs a backlog of N parked
/// reads N wakes per append: an audio backlog of ~800 one-frame groups pins a runtime.
pub(crate) struct Wakes {
	/// The earliest parked deadline in floored micros, or `u64::MAX`, so a write that
	/// crosses nothing skips the lock.
	///
	/// Relaxed: a read registers before resolving the edge, under the group locks a
	/// write takes before it presents, so a write the read missed sees its deadline.
	earliest: AtomicU64,
	parked: kio::Lock<Parked>,
}

#[derive(Default)]
struct Parked {
	/// Reads by deadline, in floored micros: never later than the exact deadline, so a
	/// write that crosses one is never skipped.
	deadlines: BTreeMap<u64, kio::WaiterList>,
	/// Reads by their group's sequence, woken when a group lands above them.
	landings: BTreeMap<u64, kio::WaiterList>,
	/// `landings` for groups held from another copy, also woken by their own group
	/// landing here. Kept apart because this track may never have held them, so falling
	/// below its oldest group does not end them; their readers leaving does.
	held: BTreeMap<u64, kio::WaiterList>,
	/// The `held` size that sweeps out the lists every reader has left.
	held_sweep: usize,
}

/// The smallest `held` size worth sweeping.
const HELD_SWEEP_MIN: usize = 16;

impl Default for Wakes {
	fn default() -> Self {
		Self {
			earliest: AtomicU64::new(u64::MAX),
			parked: kio::Lock::default(),
		}
	}
}

impl Wakes {
	/// A frame write left its group presenting up to `latest`: wake the reads it expires.
	pub(crate) fn presented(&self, latest: Timestamp) {
		let at = micros(latest.as_micros());
		if at < self.earliest.load(Ordering::Relaxed) {
			return;
		}
		let mut woken = Vec::new();
		{
			let mut parked = self.parked.lock();
			while let Some(entry) = parked.deadlines.first_entry()
				&& *entry.key() <= at
			{
				woken.push(entry.remove());
			}
			let earliest = parked.deadlines.first_key_value().map_or(u64::MAX, |(at, _)| *at);
			self.earliest.store(earliest, Ordering::Relaxed);
		}
		woken.iter_mut().for_each(kio::WaiterList::wake);
	}

	/// Park `waiter` until the edge reaches `reach` plus `budget`.
	pub(crate) fn watch_deadline(&self, reach: Timestamp, budget: Duration, waiter: &kio::Waiter) {
		let deadline = micros(reach.as_micros()).saturating_add(micros(budget.as_micros()));
		// No timestamp reaches it.
		if deadline == u64::MAX {
			return;
		}
		let mut parked = self.parked.lock();
		parked.deadlines.entry(deadline).or_default().register(waiter);
		self.earliest.fetch_min(deadline, Ordering::Relaxed);
	}

	/// Park `waiter` until a group lands above `sequence`.
	pub(crate) fn watch_landing(&self, sequence: u64, waiter: &kio::Waiter) {
		self.parked
			.lock()
			.landings
			.entry(sequence)
			.or_default()
			.register(waiter);
	}

	/// Park `waiter`, holding group `sequence` from another copy, until it or a group
	/// above it lands.
	pub(crate) fn watch_held(&self, sequence: u64, waiter: &kio::Waiter) {
		let mut parked = self.parked.lock();
		let parked = &mut *parked;
		// Sweep only once the map has doubled, so each sweep is paid for by the keys
		// added before it: a switch holding N groups costs O(N), not O(N²). Here rather
		// than in `landed`, which every insert takes.
		if parked.held.len() >= parked.held_sweep {
			parked.held.retain(|_, list| !list.is_empty());
			parked.held_sweep = (parked.held.len() * 2).max(HELD_SWEEP_MIN);
		}
		parked.held.entry(sequence).or_default().register(waiter);
	}

	/// A servable group landed at `sequence`, so it succeeds every read from `below`, the
	/// nearest servable group beneath it, up, and continues a held group of its own
	/// sequence. Reads below `oldest`, the first group still cached, can no longer expire
	/// and are forgotten; held groups are not.
	pub(crate) fn landed(&self, below: Option<u64>, sequence: u64, oldest: u64) {
		let mut woken = Vec::new();
		{
			let mut parked = self.parked.lock();
			let parked = &mut *parked;
			while let Some(entry) = parked.landings.first_entry()
				&& *entry.key() < oldest
			{
				entry.remove();
			}
			let from = below.unwrap_or(0);
			while let Some(at) = parked.landings.range(from..sequence).next().map(|(at, _)| *at) {
				woken.extend(parked.landings.remove(&at));
			}
			while let Some(at) = parked.held.range(from..=sequence).next().map(|(at, _)| *at) {
				woken.extend(parked.held.remove(&at));
			}
		}
		woken.iter_mut().for_each(kio::WaiterList::wake);
	}

	/// Wake every parked read, for a change that moves every budget at once.
	pub(crate) fn wake_all(&self) {
		let parked = {
			let mut parked = self.parked.lock();
			self.earliest.store(u64::MAX, Ordering::Relaxed);
			std::mem::take(&mut *parked)
		};
		let lists = parked.deadlines.into_values().chain(parked.landings.into_values());
		for mut list in lists.chain(parked.held.into_values()) {
			list.wake();
		}
	}
}

/// Saturate a microsecond count into a deadline key.
fn micros(value: u128) -> u64 {
	value.try_into().unwrap_or(u64::MAX)
}

/// An [`Arrival`] or the [`Wall`] clock before any pass has dated it.
const UNDATED: u64 = u64::MAX;

/// The smallest `dirty` size worth sweeping for tracks that are gone.
const DIRTY_SWEEP_MIN: usize = 64;

/// When a group became servable at this hop, in nanoseconds on its pool's [`Wall`].
///
/// Dated by the first pass after it lands instead of by reading a clock, so it never reads
/// earlier than the landing: a late pass only makes the group look younger.
pub(crate) struct Arrival(AtomicU64);

impl Default for Arrival {
	fn default() -> Self {
		Self(AtomicU64::new(UNDATED))
	}
}

impl Arrival {
	/// The arrival, or `None` until a pass dates it.
	fn get(&self) -> Option<u64> {
		let at = self.0.load(Ordering::Relaxed);
		(at != UNDATED).then_some(at)
	}

	/// Date the arrival at `at` if no pass has yet, returning whether this one did.
	pub(crate) fn date(&self, at: u64) -> bool {
		self.0
			.compare_exchange(UNDATED, at, Ordering::Relaxed, Ordering::Relaxed)
			.is_ok()
	}
}

/// A cache pool's wall clock, advanced by its passes ([`crate::cache::Pool::gc`]), and the
/// reads parked on it until a successor's arrival is a full budget old.
///
/// One per pool rather than per track, so a pass finds the earliest deadline without
/// visiting every track. It visits only the tracks where a group landed since the last one.
pub(crate) struct Wall {
	/// The last pass, in nanoseconds from the first, or [`UNDATED`] before it.
	now: AtomicU64,
	state: kio::Lock<WallState>,
}

#[derive(Default)]
struct WallState {
	/// The first pass, which the nanosecond counts are measured from.
	epoch: Option<Instant>,
	/// Reads by the wall time they go stale at.
	deadlines: BTreeMap<u64, kio::WaiterList>,
	/// Tracks where a group landed since the last pass, which the next one dates.
	dirty: Vec<Weak<cache::Track>>,
	/// The `dirty` size that sweeps out the tracks that are gone, for a pool nobody passes.
	dirty_sweep: usize,
	/// Reads waiting for an undated arrival.
	dating: kio::WaiterList,
	/// The drivers that pass, woken when a landing or an earlier deadline needs one sooner.
	drivers: kio::WaiterList,
}

impl Default for Wall {
	fn default() -> Self {
		Self {
			now: AtomicU64::new(UNDATED),
			state: Default::default(),
		}
	}
}

impl Wall {
	/// A group landed in `track`, the first since the last pass: have the next pass date it,
	/// and wake the drivers for that pass.
	pub(crate) fn landed(&self, track: Weak<cache::Track>) {
		let mut drivers = {
			let mut state = self.state.lock();
			let state = &mut *state;
			if state.dirty.len() >= state.dirty_sweep {
				state.dirty.retain(|track| track.strong_count() > 0);
				state.dirty_sweep = (state.dirty.len() * 2).max(DIRTY_SWEEP_MIN);
			}
			state.dirty.push(track);
			match state.dirty.len() {
				1 => state.drivers.take(),
				_ => kio::WaiterList::new(),
			}
		};
		drivers.wake();
	}

	/// Whether a group that arrived at `arrival` is at least `budget` old on this clock.
	/// An undated arrival is only known to be at least zero.
	pub(crate) fn aged(&self, arrival: &Arrival, budget: Duration) -> bool {
		if budget.is_zero() {
			return true;
		}
		let now = self.now.load(Ordering::Relaxed);
		match arrival.get() {
			Some(at) if now != UNDATED => now.saturating_sub(at) >= nanos(budget),
			_ => false,
		}
	}

	/// Park `waiter` until a group that arrived at `arrival` is `budget` old: on its deadline,
	/// or first on the pass that dates it, which its landing already asked for.
	///
	/// Registered before the caller judges [`Self::aged`], so a pass in between is either
	/// seen there or wakes the read.
	pub(crate) fn watch(&self, arrival: &Arrival, budget: Duration, waiter: &kio::Waiter) {
		let mut drivers = {
			let mut state = self.state.lock();
			let Some(at) = arrival.get() else {
				state.dating.register(waiter);
				return;
			};
			let deadline = at.saturating_add(nanos(budget));
			if deadline == u64::MAX {
				return;
			}
			let earliest = state
				.deadlines
				.first_key_value()
				.is_none_or(|(first, _)| deadline < *first);
			state.deadlines.entry(deadline).or_default().register(waiter);
			match earliest {
				true => state.drivers.take(),
				false => kio::WaiterList::new(),
			}
		};
		drivers.wake();
	}

	/// Move the clock to `now`: date every group that landed since the last pass, and wake
	/// the reads waiting on a date or past their deadline. Returns the next deadline.
	pub(crate) fn pass(&self, now: Instant) -> Option<Instant> {
		let mut state = self.state.lock();
		let epoch = *state.epoch.get_or_insert(now);
		let at = nanos(now.saturating_duration_since(epoch));
		self.now.store(at, Ordering::Relaxed);
		if !state.dirty.is_empty() {
			let dirty = std::mem::take(&mut state.dirty);
			// Outside the lock: dating takes each track's, under which its landings take this one.
			drop(state);
			for track in dirty.iter().filter_map(Weak::upgrade) {
				track.date(at);
			}
			state = self.state.lock();
		}
		let mut dating = state.dating.take();
		let mut due = Vec::new();
		while let Some(entry) = state.deadlines.first_entry()
			&& *entry.key() <= at
		{
			due.push(entry.remove());
		}
		let next = state.deadline();
		drop(state);
		dating.wake();
		due.iter_mut().for_each(kio::WaiterList::wake);
		next
	}

	/// Register a driver's `waiter` for a pass wanted sooner than the deadline it was last
	/// told, returning when the next pass is due: now, while landed groups wait for a date.
	pub(crate) fn park(&self, waiter: &kio::Waiter) -> Option<Instant> {
		let mut state = self.state.lock();
		state.drivers.register(waiter);
		match state.dirty.is_empty() {
			true => state.deadline(),
			false => state
				.epoch
				.map(|epoch| epoch + Duration::from_nanos(self.now.load(Ordering::Relaxed))),
		}
	}
}

impl WallState {
	/// The earliest parked deadline as an instant.
	fn deadline(&self) -> Option<Instant> {
		let epoch = self.epoch?;
		let (at, _) = self.deadlines.first_key_value()?;
		epoch.checked_add(Duration::from_nanos(*at))
	}
}

/// Saturate a duration into nanoseconds on a [`Wall`].
fn nanos(duration: Duration) -> u64 {
	duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod test {
	use super::*;
	use std::sync::Arc;

	/// A waiter that records whether it was woken.
	fn flagged() -> (Arc<std::sync::atomic::AtomicBool>, kio::Waiter) {
		struct Flag(Arc<std::sync::atomic::AtomicBool>);
		impl std::task::Wake for Flag {
			fn wake(self: Arc<Self>) {
				self.0.store(true, Ordering::SeqCst);
			}
		}
		let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let waiter = kio::Waiter::new(std::task::Waker::from(Arc::new(Flag(flag.clone()))));
		(flag, waiter)
	}

	/// A track whose groups land in `pool`.
	fn track(pool: &cache::Pool) -> crate::track::Producer {
		let info = crate::broadcast::Info {
			pool: pool.clone(),
			..Default::default()
		};
		crate::track::Producer::new(Arc::new(info), "test", crate::track::Info::default())
	}

	/// A landing asks the driver for the pass that dates it, so the group's age never
	/// waits on the driver's next scheduled deadline.
	#[test]
	fn a_landing_wakes_the_driver_for_its_pass() {
		let pool = cache::Pool::unbounded();
		let start = crate::model::clock::now();
		assert_eq!(pool.gc(start), None);
		let (woken, driver) = flagged();
		assert_eq!(pool.park(&driver), None, "nothing to wait for");

		let track = track(&pool);
		track.append_group().unwrap();
		assert!(
			woken.swap(false, Ordering::SeqCst),
			"the landing lost the driver's wakeup"
		);
		assert_eq!(pool.park(&driver), Some(start), "an undated landing wants a pass now");
		track.append_group().unwrap();
		assert!(
			!woken.load(Ordering::SeqCst),
			"one pass dates every landing since the last"
		);

		pool.gc(start + Duration::from_millis(5));
		assert_eq!(pool.park(&driver), None, "dated");
	}

	/// Only a deadline earlier than the one the driver sleeps until wakes it.
	#[test]
	fn an_earlier_deadline_wakes_the_driver() {
		let wall = Wall::default();
		let start = crate::model::clock::now();
		wall.pass(start);
		let arrival = Arrival::default();
		assert!(arrival.date(0));
		assert!(!arrival.date(1), "dated once");
		let (_, reader) = flagged();
		let (woken, driver) = flagged();

		wall.park(&driver);
		wall.watch(&arrival, Duration::from_secs(2), &reader);
		assert!(woken.swap(false, Ordering::SeqCst));
		assert_eq!(wall.park(&driver), Some(start + Duration::from_secs(2)));
		wall.watch(&arrival, Duration::from_secs(3), &reader);
		assert!(
			!woken.load(Ordering::SeqCst),
			"a later deadline must not wake the driver"
		);
		wall.watch(&arrival, Duration::from_secs(1), &reader);
		assert!(woken.load(Ordering::SeqCst));
		assert_eq!(wall.park(&driver), Some(start + Duration::from_secs(1)));
	}

	/// An undated arrival is only known to be at least zero old, and a pool nobody passes
	/// keeps no more than its live tracks for a pass that never comes.
	#[test]
	fn an_undriven_pool_keeps_only_its_live_tracks() {
		let pool = cache::Pool::unbounded();
		let undated = Arrival::default();
		assert!(pool.wall().aged(&undated, Duration::ZERO));
		assert!(!pool.wall().aged(&undated, Duration::from_nanos(1)));

		let _live = track(&pool);
		for _ in 0..1000 {
			track(&pool).append_group().unwrap();
		}
		let dirty = pool.wall().state.lock().dirty.len();
		assert!(dirty <= 2 * DIRTY_SWEEP_MIN, "{dirty} tracks kept for a pass");
	}

	/// Each route switch parks the groups then in flight; once their readers drop, the
	/// next switch's park must not find them still indexed.
	#[test]
	fn held_groups_do_not_accumulate_across_route_switches() {
		let wakes = Wakes::default();
		let mut reader = None;
		for sequence in 0..100 {
			// The last switch's reader gave its group up.
			drop(reader.take());
			let waiter = kio::Waiter::noop();
			wakes.watch_held(sequence, &waiter);
			reader = Some(waiter);
		}
		assert!(
			wakes.parked.lock().held.len() <= HELD_SWEEP_MIN,
			"the readers that left are swept"
		);
	}
}
