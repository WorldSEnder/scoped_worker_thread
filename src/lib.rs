//! A worker thread is similar to a [thread scope] in that it can be sent units of work.
//!
//! Every worker is owned by whichever thread has spawned it and can be sent
//! closures to work on, and sends their results back. The main feature is that these closures
//! are allowed to borrow (mutably) memory from the spawning thread and synchronize
//! after the work is finished to release the borrow.
//!
//! This is mainly useful when repeatedly performing work you want to parallelize without having
//! to pay the cost of starting and stopping new threads every time a `scope` is entered.
//!
//! # Example
//!
//! ```
//! use scoped_worker_thread::{Worker, scope};
//!
//! let mut worker1 = Worker::spawn();
//! let mut worker2 = Worker::spawn();
//!
//! // Store these workers somewhere, then later ....
//!
//! let mut result = [0; 4];
//! let (left, right) = result.split_at_mut(2);
//! scope(|scope| {
//!     scope.send(&mut worker1, || {
//!         // This work happens on the first worker's thread
//!         left.fill(1);
//!     });
//!     scope.send(&mut worker2, || {
//!         // This work happens on the second worker's thread
//!         right.fill(2);
//!     });
//! });
//! assert_eq!(result, [1, 1, 2, 2]);
//!
//! // ... again later, clean up the workers.
//!
//! worker1.join();
//! worker2.join();
//! ```
//!
//! [thread scope]: std::thread::scope

use std::marker::PhantomData;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{current, park, Thread};

mod worker;

pub use worker::Worker;
use worker::{new_packet, ClaimedWorker};

/// An owned permission to wait for a piece of work to finish by blocking.
///
/// See [Scope::send] for details.
pub struct ScopedJoinHandle<'scope, T> {
    result: ClaimedWorker<'scope, T>,
    scope: PhantomData<&'scope mut &'scope ()>,
}

impl<'scope, T> ScopedJoinHandle<'scope, T> {
    /// Waits for the associated work to finish.
    ///
    /// The will return immediately if the associated work has already been completed.
    ///
    /// For memory ordering, all operations performed in the work item have a [*happens before*] relationship
    /// with the function returning.
    ///
    /// If the associated thread panics, [`Err`] is returned with the panic payload.
    ///
    /// [*happens before*]: https://doc.rust-lang.org/nomicon/atomics.html#data-accesses
    pub fn join(self) -> std::thread::Result<T> {
        self.result.join()
    }
}

struct ScopeData {
    fail_flag: AtomicBool,
    num_active_workers: AtomicUsize,
    main_thread: Thread,
}

impl ScopeData {
    fn start_worker(&self) {
        // This store has a happens-before relationship with the corresponding subtraction in `finish_worker`
        // via the passing of the work item over the shared channel. Notice that we first count up,
        // and then send the work.
        let active_count = self.num_active_workers.fetch_add(1, Ordering::Relaxed);
        // This is the only thread that adds to the count, hence this check is sufficient.
        if active_count + 1 == usize::MAX {
            self.overflow()
        }
    }
    #[cold]
    fn overflow(&self) {
        panic!("too many active workers");
    }
    fn finish_worker(&self, unwinding: bool) {
        if unwinding {
            self.fail_flag.store(true, Ordering::Relaxed);
        }
        // In case the join handle is simply dropped, this would not happen-before the load in `wait_for_workers`.
        // Hence, we use Release semantics here. For loading, relaxed is sufficient, see comment in `start_worker`.
        if self.num_active_workers.fetch_sub(1, Ordering::Release) == 1 {
            // Unpark if we were the last running thread
            self.main_thread.unpark();
        }
    }
    fn wait_for_workers(&self) -> bool {
        debug_assert!(
            current().id() == self.main_thread.id(),
            "must be called from main thread"
        );
        while self.num_active_workers.load(Ordering::Acquire) != 0 {
            park();
        }
        // Ordering is done by waiting for the workers above
        self.fail_flag.load(Ordering::Relaxed)
    }
}

/// A scope during which to perform work.
///
/// See [`scope`] for details.
pub struct Scope<'scope, 'env>
where
    'env: 'scope,
{
    data: &'scope ScopeData,
    // invariant over 'scope
    scope: PhantomData<&'scope mut &'scope ()>,
    env: PhantomData<&'env mut &'env ()>,
}

impl<'scope, 'env> Scope<'scope, 'env> {
    // TODO: we could - either unsafely, or with a RefCell - allow a worker to work on multiple pieces of work.
    //       The current design takes unique ownership of the worker thread and *must* wait for that piece of work to complete
    //       before `join` can return. Since there is no causal relation between joining the handle and the worker borrow, we can't
    //       safely "return" the borrow to the worker until the end-of-scope. Since dropping the join handle will *not* wait for
    //       the work to finish, we can't 'just' tack on another lifetime onto the handle to establish this ownership and must pretend
    //       to always hold it until the end of the scope.
    /// Send some work to a worker, returning a [`ScopedJoinHandle`] for it.
    ///
    /// The closure can borrow non-`'static` data from outside the scope.
    ///
    /// The join handle has a [`join`](ScopedJoinHandle::join) method to synchronously wait for the submitted task to complete. If the
    /// task panics, joining it will return an [`Err`].
    ///
    /// If the join handle is dropped without joining it, the task will be implicitly joined at the end of the scope. In this case,
    /// [`scope`] will panic after all workers have finished.
    ///
    /// Note that no additional threads are spawned. Every [`Worker`] can only be used once per [`scope`] and work on one unit of work.
    pub fn send<T, F>(&self, worker: &'scope mut Worker, task: F) -> ScopedJoinHandle<'scope, T>
    where
        F: 'scope + Send + FnOnce() -> T,
        T: 'scope + Send,
    {
        // We could bump-alloc from a buffer in Scope. Since threads are basically not usable without `alloc`, we are lazy and use a box.
        let packet = new_packet(task, |unwinding| self.data.finish_worker(unwinding));
        self.data.start_worker();
        let result = worker.submit(packet);
        ScopedJoinHandle {
            result,
            scope: PhantomData,
        }
    }
}

/// Create a scope for submitting units-of-work to [`Worker`]s.
///
/// The function passed to [`scope`] will be provided a [`Scope`] object, through which scoped units of work can be submitted.
///
/// All work will be finished on the worker threads by the time [`scope`] returns, but the worker threads will continue running.
///
/// Work closures can borrow non-`'static` data, as the scope guarantees that all borrows are returned at the end of the scope and are non-overlapping.
///
/// All work that has not been explicitly [joined](ScopedJoinHandle::join) will be implicitly waited for at the end of the scope.
///
/// # Example
///
/// ```
/// use scoped_worker_thread::{Worker, scope};
///
/// let mut worker1 = Worker::spawn();
/// let mut worker2 = Worker::spawn();
/// let mut result = [0; 4];
/// let (left, right) = result.split_at_mut(2);
/// scope(|scope| {
///     scope.send(&mut worker1, || {
///         left.fill(1);
///     });
///     scope.send(&mut worker2, || {
///         right.fill(2);
///     });
/// });
/// assert_eq!(result, [1, 1, 2, 2]);
/// worker1.join();
/// worker2.join();
/// ```
pub fn scope<'env, F, T>(f: F) -> T
where
    F: 'env + for<'scope> FnOnce(Scope<'scope, 'env>) -> T,
    T: 'env,
{
    let scope_data = ScopeData {
        // When other threads execute a payload, they either unwind or terminate the process entirely.
        fail_flag: AtomicBool::new(false),
        num_active_workers: AtomicUsize::new(0),
        main_thread: current(),
    };
    // 'scope starts here
    let scope: Scope<'_, 'env> = Scope {
        data: &scope_data,
        scope: PhantomData,
        env: PhantomData,
    };
    let main_result = catch_unwind(AssertUnwindSafe(|| f(scope)));
    let a_worker_failed = scope_data.wait_for_workers();
    match main_result {
        Err(e) => resume_unwind(e),
        Ok(_) if a_worker_failed => {
            // Because one of the work items has triggered a panic, we have to do the same thing. Note that we
            // can only land here if unwinding is enabled.
            panic!("One of the work items has panicked!");
        }
        Ok(main_result) => main_result,
    }
    // 'scope ends here
}

#[cfg(test)]
mod tests;
