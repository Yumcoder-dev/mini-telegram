use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Instant};
use tracing::debug;

/// A wrapper around a `Db` instance. This exists to allow orderly cleanup
/// of the `Db` by signalling the background purge task to shut down when
/// this struct is dropped.
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    /// The `Db` instance that will be shut down when this `DbHolder` struct
    /// is dropped.
    db: Db,
}

/// Server state shared across all connections.
///
/// `Db` contains a `HashMap` storing the key/value data and all
/// `broadcast::Sender` values for active pub/sub channels.
///
/// A `Db` instance is a handle to shared state. Cloning `Db` is shallow and
/// only incurs an atomic ref count increment.
///
/// When a `Db` value is created, a background task is spawned. This task is
/// used to expire values after the requested duration has elapsed. The task
/// runs until all instances of `Db` are dropped, at which point the task
/// terminates.
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// Handle to shared state. The background task will also have an
    /// `Arc<Shared>`.
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// The shared state is guarded by a mutex. This is a `std::sync::Mutex` and
    /// not a Tokio mutex. This is because there are no asynchronous operations
    /// being performed while holding the mutex. Additionally, the critical
    /// sections are very small.
    ///
    /// A Tokio mutex is mostly intended to be used when locks need to be held
    /// across `.await` yield points. All other cases are **usually** best
    /// served by a std mutex. If the critical section does not include any
    /// async operations but is long (CPU intensive or performing blocking
    /// operations), then the entire operation, including waiting for the mutex,
    /// is considered a "blocking" operation and `tokio::task::spawn_blocking`
    /// should be used.
    state: Mutex<State>,

    /// Notifies the background task handling entry expiration. The background
    /// task waits on this to be notified, then checks for expired values or the
    /// shutdown signal.
    background_task: Notify,
}

/// Entry in the key-value store
#[derive(Debug, PartialEq)]
struct Entry {
    /// Uniquely identifies this entry.
    id: u64,

    /// Stored data
    data: Bytes, // A cheaply cloneable and sliceable chunk of contiguous memory.

    /// Instant at which the entry expires and should be removed from the
    /// database.
    expires_at: Option<Instant>,
}

#[derive(Debug, Default)]
struct State {
    /// The key-value data. We are not trying to do anything fancy so a
    /// `std::collections::HashMap` works fine.
    entries: HashMap<String, Entry>,

    /// The pub/sub key-space. telegram uses a **separate** key space for key-value
    /// and pub/sub. `mini-telegram` handles this by using a separate `HashMap`.
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// Tracks key TTLs.
    ///
    /// A `BTreeMap` is used to maintain expirations sorted by when they expire.
    /// This allows the background task to iterate this map to find the value
    /// expiring next.
    ///
    /// While highly unlikely, it is possible for more than one expiration to be
    /// created for the same instant. Because of this, the `Instant` is
    /// insufficient for the key. A unique expiration identifier (`u64`) is used
    /// to break these ties.
    expirations: BTreeMap<(Instant, u64), String>,

    /// Identifier to use for the next expiration. Each expiration is associated
    /// with a unique identifier. See above for why.
    next_id: u64,

    /// True when the Db instance is shutting down. This happens when all `Db`
    /// values drop. Setting this to `true` signals to the background task to
    /// exit.
    shutdown: bool,
}

impl DbDropGuard {
    /// Create a new `DbHolder`, wrapping a `Db` instance. When this is dropped
    /// the `Db`'s purge task will be shut down.
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// Get the shared database. Internally, this is an
    /// `Arc`, so a clone only increments the ref count.
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // Signal the 'Db' instance to shut down the task that purges expired keys
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// Create a new, empty, `Db` instance. Allocates shared state and spawns a
    /// background task to manage key expiration.
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Default::default(),
            background_task: Notify::new(),
        });

        // Start the background task.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// Get the value associated with a key.
    ///
    /// Returns `None` if there is no value associated with the key. This may be
    /// due to never having assigned a value to the key or a previously assigned
    /// value expired.
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // Acquire the lock, get the entry and clone the value.
        //
        // Because data is stored using `Bytes`, a clone here is a shallow
        // clone. Data is not copied.
        let state = self.shared.state.lock().unwrap();
        // map uses to convert Option<&Bytes> to Option<Bytes>
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// Set the value associated with a key along with an optional expiration
    /// Duration.
    ///
    /// If a value is already associated with the key, it is removed.
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // Get and increment the next insertion ID. Guarded by the lock, this
        // ensures a unique identifier is associated with each `set` operation.
        let id = state.next_id;
        state.next_id += 1;

        // If this `set` becomes the key that expires **next**, the background
        // task needs to be notified so it can update its state.
        //
        // Whether or not the task needs to be notified is computed during the
        // `set` routine.
        let mut notify = false;
        // note that expire.map() function invokes if expire has a value (not equal to None).
        let expires_at = expire.map(|duration| {
            // `Instant` at which the key expires.
            let when = Instant::now() + duration;

            // Only notify the worker task if the newly inserted expiration is the
            // **next** key to evict. In this case, the worker needs to be woken up
            // to update its state.
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                // why default is true?
                // if next_expiration() returns None (background worker in wait state) and
                // expire is not None, so reschedule the background worker
                .unwrap_or(true);

            // Track the expiration.
            state.expirations.insert((when, id), key.clone());
            when
        });

        // Insert the entry into the `HashMap`.
        // let mut map = HashMap::new();
        // assert_eq!(map.insert(37, "a"), None);
        // assert_eq!(map.insert(37, "b"), Some("a"));
        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: value,
                expires_at,
            },
        );

        // If there was a value previously associated with the key **and** it
        // had an expiration time. The associated entry in the `expirations` map
        // must also be removed. This avoids leaking data.
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // clear expiration
                state.expirations.remove(&(when, prev.id));
            }
        }

        // Release the mutex before notifying the background task. This helps
        // reduce contention by avoiding the background task waking up only to
        // be unable to acquire the mutex due to this function still holding it.
        drop(state);

        if notify {
            // Finally, only notify the background task if it needs to update
            // its state to reflect a new expiration.
            self.shared.background_task.notify_one();
        }
    }

    /// Returns a `Receiver` for the requested channel.
    ///
    /// The returned `Receiver` is used to receive values broadcast by `PUBLISH`
    /// commands.
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // Acquire the mutex
        let mut state = self.shared.state.lock().unwrap();

        // If there is no entry for the requested channel, then create a new
        // broadcast channel and associate it with the key. If one already
        // exists, return an associated receiver.
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // No broadcast channel exists yet, so create one.
                //
                // The channel is created with a capacity of `1024` messages. A
                // message is stored in the channel until **all** subscribers
                // have seen it. This means that a slow subscriber could result
                // in messages being held indefinitely.
                //
                // When the channel's capacity fills up, publishing will result
                // in old messages being dropped. This prevents slow consumers
                // from blocking the entire system.
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// Publish a message to the channel. Returns the number of subscribers
    /// listening on the channel.
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // On a successful message send on the broadcast channel, the number
            // of subscribers is returned. An error indicates there are no
            // receivers, in which case, `0` should be returned.
            .map(|tx| tx.send(value).unwrap_or(0))
            // If there is no entry for the channel key, then there are no
            // subscribers. In this case, return `0`.
            .unwrap_or(0)
    }

    /// Signals the purge background task to shut down. This is called by the
    /// `DbShutdown`s `Drop` implementation.
    fn shutdown_purge_task(&self) {
        // The background task must be signaled to shut down. This is done by
        // setting `State::shutdown` to `true` and signalling the task.
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // Drop the lock before signalling the background task. This helps
        // reduce lock contention by ensuring the background task doesn't
        // wake up only to be unable to acquire the mutex.
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// Purge all expired keys and return the `Instant` at which the **next**
    /// key will expire. The background task will sleep until this instant.
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            // The database is shutting down.
            // All handles to the shared state have dropped.
            // So, the background task should exit immediately.
            return None;
        }

        // This is needed to make the borrow checker happy. In short, `lock()`
        // returns a `MutexGuard` and not a `&mut State`. The borrow checker is
        // not able to see "through" the mutex guard and determine that it is
        // safe to access both `state.expirations` and `state.entries` mutably,
        // so we get a "real" mutable reference to `State` outside of the loop.
        //
        // - while ... state.expirations.iter().next() <-- next(): state.entries state.expirations
        // - state.entries.remove(key); <-- remove(): state.entries to state.entries
        let state = &mut *state;

        // Find all keys scheduled to expire **before** now.
        let now = Instant::now();
        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            // *
            if when > now {
                // Done purging, `when` is the instant at which the next key
                // expires. The worker task will wait until this instant.
                return Some(when);
            }

            // The key expired, remove it
            state.entries.remove(key);
            state.expirations.remove(&(when, id));
        }

        None
    }

    /// Returns `true` if the database is shutting down
    ///
    /// The `shutdown` flag is set when all `Db` values have dropped, indicating
    /// that the shared state can no longer be accessed.
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    /// Returns `Option<Instant>` of the next expiration key
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .keys()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// Routine executed by the background task
///
/// Wait to be notified. On notification, purge any expired keys form the shared_cloned
/// state handle. If 'shutdown' is set, terminate the task.
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // if the shoutdown flag is set, then the task should exist.
    while !shared.is_shutdown() {
        // Purge all keys that are expired.
        // The function returns the instat at which the **next** key will expire.
        // The worker should wait until the instant has passed thenpurge again.
        if let Some(when) = shared.purge_expired_keys() {
            // Wait until the next key expires **or** until the background task
            // is notified. If the task is notified, then it must reload its
            // state as new keys have been set to expire early. This is done by
            // looping.
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // There are no keys expiring in the future.
            // wait until the task is notified
            shared.background_task.notified().await;
        }
    }

    debug!("Purge background task shut down")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    extern crate test;
    // use test::Bencher;

    #[test]
    fn test_hash_map() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert("a", 1);
        map.insert("b", 2);
        map.insert("c", 3);

        assert_eq!(map.get(&"a"), Some(&1));
        assert_eq!(map.get(&"b"), Some(&2));
        assert_eq!(map.get(&"c"), Some(&3));
    }

    #[tokio::test]
    async fn test_broadcast() {
        //                      ┌──────────────┐
        //                 ┌────►   thread x   │
        //                 │    └──────────────┘
        // ┌────────────┐  │
        // │  thread z  ├──┤
        // └────────────┘  │    ┌──────────────┐
        //     [10,20]     └────►   thread y   │
        //                      └──────────────┘
        //
        let (tx, mut rx1) = broadcast::channel(16);
        let mut rx2 = tx.subscribe();
        tokio::spawn(async move {
            // thread x
            assert_eq!(rx1.recv().await.unwrap(), 10);
            assert_eq!(rx1.recv().await.unwrap(), 20);
        });
        tokio::spawn(async move {
            // thread y
            assert_eq!(rx2.recv().await.unwrap(), 10);
            assert_eq!(rx2.recv().await.unwrap(), 20);
        });

        // thread z
        tx.send(10).unwrap();
        tx.send(20).unwrap();
    }

    #[tokio::test]
    async fn test_broadcast_capacity_fills_up() {
        use tokio::time::{sleep, Duration};

        let (sender, mut receiver) = broadcast::channel(2);

        let handler = tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            for _ in 0..3 {
                let x = receiver.recv().await;
                match x {
                    Ok(i) => {
                        // only received the last 2(chanel capacity) data
                        assert!(i == 8 || i == 9)
                    }
                    Err(/* e */ _) => {
                        // println!("received err:{}", e);
                        break;
                    }
                }
            }
        });

        for i in 0..10 {
            let res = sender.send(i);
            match res {
                Ok(_) => { /*println!("sent")*/ }
                Err(e) => println!("send err:{}", e),
            }
        }

        handler.await.unwrap();
    }

    #[test]
    fn test_arc_mutex_lock_1() {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let mutex = Arc::new(Mutex::new(0));
        let c_mutex = Arc::clone(&mutex);

        thread::spawn(move || {
            *c_mutex.lock().unwrap() = 10;
        })
        .join()
        .expect("thread::spawn failed");

        assert_eq!(*mutex.lock().unwrap(), 10);
    }

    #[test]
    fn test_arc_mutex_lock_2() {
        use std::sync::mpsc::channel;
        use std::sync::{Arc, Mutex};
        use std::thread;

        const N: usize = 10;

        // Spawn a few threads to increment a shared variable (non-atomically), and
        // let the main thread know once all increments are done.
        //
        // Here we're using an Arc to share memory among threads, and the data inside
        // the Arc is protected with a mutex.
        let data = Arc::new(Mutex::new(0));

        let (tx, rx) = channel();
        for _ in 0..N {
            let (data, tx) = (Arc::clone(&data), tx.clone());
            thread::spawn(move || {
                // The shared state can only be accessed once the lock is held.
                // Our non-atomic increment is safe because we're the only thread
                // which can access the shared state when the lock is held.
                //
                // We unwrap() the return value to assert that we are not expecting
                // threads to ever fail while holding the lock.
                let mut data = data.lock().unwrap();
                *data += 1;
                if *data == N {
                    tx.send(()).unwrap();
                }
                // the lock is unlocked here when `data` goes out of scope.
            });
        }

        assert_eq!(rx.recv().unwrap(), ());
    }

    #[tokio::test]
    async fn test_instant() {
        // TODO: Add example for different between std::Instant and tokio::Instant
        use tokio::time::{sleep, Duration, Instant};

        let now = Instant::now();
        sleep(Duration::new(1, 0)).await;
        let new_now = Instant::now();
        // assert_eq!(new_now.checked_duration_since(now).unwrap().as_secs(), 1);
        assert!(new_now.duration_since(now) >= Duration::from_secs(1));
    }

    #[test]
    fn test_tuple() {
        let tuple = (1, "hello", 4.5, true);
        let (a, b, c, d) = tuple;
        // println!("{:?}, {:?}, {:?}, {:?}", a, b, c, d);

        assert_eq!(a, 1);
        assert_eq!(b, "hello");
        assert_eq!(c, 4.5);
        assert!(d);
    }

    #[tokio::test]
    async fn test_notify() {
        // use tokio::time::sleep;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        // `Notify` can be thought of as a [`Semaphore`] starting with 0 permits.
        // [`notified().await`] waits for a permit to become available, and [`notify_one()`]
        // sets a permit **if there currently are no available permits**.
        let notify = Arc::new(Notify::new());
        let notify2 = notify.clone();

        // https://doc.rust-lang.org/rust-by-example/std/rc.html
        // let shared = Arc::new(Mutex::new(1));
        let shared = Arc::new(AtomicUsize::new(1));

        let shared_cloned = Arc::clone(&shared);
        let handle = tokio::spawn(async move {
            // println!("start worker thread...");
            notify2.notified().await;
            // println!("received notification");
            // let mut data = c_shared.lock().await;
            // *data = 10;
            shared_cloned.store(10, Ordering::SeqCst);
        });
        // sleep(Duration::from_millis(100)).await;
        // println!("sending notification");
        notify.notify_one();

        handle.await.unwrap(); // wait until the thread end
                               // assert_eq!(*shared.lock().await, 10);
        assert_eq!(10, shared.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_semaphore() {
        use tokio::sync::{Semaphore, TryAcquireError};
        let semaphore = Semaphore::new(3); // forget --> 2
                                           //{
                                           // 3
                                           // let a_permit = semaphore.acquire().await.unwrap().forget();
        let a_permit = semaphore.acquire().await.unwrap();
        // 2
        //}
        // 2
        let two_permits = semaphore.acquire_many(2).await.unwrap();
        assert_eq!(semaphore.available_permits(), 0);

        let permit_attempt = semaphore.try_acquire();
        assert_eq!(permit_attempt.err(), Some(TryAcquireError::NoPermits));
    }

    #[test]
    fn test_option_map() {
        let test_cases = vec![Some(Duration::from_secs(1)), None];
        let now = std::time::Instant::now();
        for t in test_cases.iter() {
            let expires_at = t.map(|duration| {
                let when = now + duration;
                when
            });
            match expires_at {
                Some(d) => {
                    assert_eq!(d, now + t.unwrap());
                }
                None => {
                    assert!(expires_at.is_none());
                }
            }
        }
    }

    #[tokio::test]
    async fn test_wait_group() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::mpsc::channel;

        const N: usize = 10;
        static GLOBAL_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

        // Create a new wait group.
        let (send, mut recv) = channel::<bool>(1);

        for _ in 0..N {
            let _sender = send.clone();
            tokio::spawn(async move {
                GLOBAL_THREAD_COUNT.fetch_add(1, Ordering::SeqCst);
                // println!("---worker--");
                drop(_sender) // release sender for current task
            });
        }

        // Wait for the tasks to finish.
        //
        // We drop our sender first because the recv() call otherwise
        // sleeps forever.
        drop(send);

        // When every sender has gone out of scope, the recv call
        // will return with an error. We ignore the error.
        let _ = recv.recv().await;

        // println!("---main/test thread---")
        assert_eq!(GLOBAL_THREAD_COUNT.load(Ordering::SeqCst), N)
    }

    #[test]
    fn test_using_state_structure() {
        let mut state: State = Default::default();
        assert!(state.entries.is_empty());
        assert_eq!(state.entries.len(), 0);

        assert!(state.pub_sub.is_empty());
        assert_eq!(state.pub_sub.len(), 0);

        assert!(state.expirations.is_empty());
        assert_eq!(state.expirations.len(), 0);

        assert_eq!(state.next_id, 0);
        assert_eq!(state.shutdown, false);

        let key = String::from("1"); // "1".into()
        let entry = Entry {
            id: 1,
            data: Bytes::from_static(b"hello"),
            expires_at: None,
        };
        // insert
        state.entries.insert(key, entry);
        assert_eq!(state.entries.len(), 1);
        // get
        let entry = state.entries.get("1").unwrap();
        assert_eq!(entry.id, 1);
        assert_eq!(entry.data, Bytes::from_static(b"hello"));
        assert_eq!(entry.expires_at, None);

        state.next_id = 10;
        assert_eq!(state.next_id, 10);

        state.shutdown = true;
        assert!(state.shutdown);
    }

    #[tokio::test]
    async fn test_db() {
        use tokio::sync::mpsc::channel;

        let db = Db::new();
        const N: usize = 10;

        // Create a new wait group.
        let (send, mut recv) = channel::<bool>(1);

        for _ in 0..N {
            let shared = db.shared.clone();
            let _sender = send.clone();
            tokio::spawn(async move {
                let mut state = shared.state.lock().unwrap();
                state.next_id += 1;
                drop(_sender)
            });
        }

        // Wait for the tasks to finish.
        //
        // We drop our sender first because the recv() call otherwise
        // sleeps forever.
        drop(send);

        // When every sender has gone out of scope, the recv call
        // will return with an error. We ignore the error.
        let _ = recv.recv().await;
        assert_eq!(db.shared.clone().state.lock().unwrap().next_id as usize, N)
    }

    #[tokio::test]
    async fn test_map_on_none() {
        let db = Db::new();
        let state = db.shared.state.lock().unwrap();
        let expire: Option<Duration> = None;

        // map on None value return None (without run the closure)
        // see map implementation in option.rs
        let notify = expire.map(|duration| {
            // println!("===== does not call at all =====");
            let when = Instant::now() + duration;
            state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true)
        });
        assert_eq!(notify, None)
    }

    #[test]
    fn test_bytes_shallow_clone() {
        use bytes::Buf;
        //     Arc ptrs                   +---------+
        //     ________________________ / | Bytes 2 |
        //    /                           +---------+
        //   /          +-----------+     |         |
        //  |_________/ |  Bytes 1  |     |         |
        //  |           +-----------+     |         |
        //  |           |           | ___/ data     | tail
        //  |      data |      tail |/              |
        //  v           v           v               v
        //  +-----+---------------------------------+-----+
        //  | Arc |     |           |               |     |
        //  +-----+---------------------------------+-----+
        let b = Bytes::from_static(b"hello yumcoder!");
        let b_clone = b.clone();
        let address_p = format!("{:p}", &b);
        let address_p_clone = format!("{:p}", &b_clone);
        assert_ne!(address_p, address_p_clone);
        let first_elem_b = format!("{:p}", &b.chunk()[0]);
        let first_elem_b_clone = format!("{:p}", &b_clone.chunk()[0]);
        assert_eq!(first_elem_b, first_elem_b_clone);
        // assert_eq!(
        //     std::ptr::addr_of!(first_elem_b),
        //     std::ptr::addr_of!(first_elem_b_clone)
        // );
    }

    #[test]
    fn test_string_clone() {
        let s = "Hello World!".to_string();
        let s_clone = s.clone();
        // println!("{:p}", &*s);
        // s.get_mut(0..5).map(|s| {
        //     s.make_ascii_uppercase();
        //     // &*s
        // });
        // println!("{:p}", s.as_ptr()); // &*s
        // println!("{:p}", s_clone.as_ptr());
        assert_ne!(s.as_ptr(), s_clone.as_ptr());
    }
}
