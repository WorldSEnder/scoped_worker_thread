//! A worker thread is similar to a [thread scope] in that it can be sent units of work.
//!
//! Every worker thread is owned by whichever thread has spawned it and can be sent
//! closures to work on sending the result back. The main feature is that these closures
//! are allowed to borrow (mutably) memory from the master thread and synchronize
//! after the work is finished to release the borrow back to the master.

use std::marker::PhantomData;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{current, park, Thread};

mod worker;

use worker::{new_packet, ClaimedWorker};
pub use worker::{JoinError, Worker};

pub struct ScopedJoinHandle<'scope, T> {
    result: ClaimedWorker<'scope, T>,
    scope: PhantomData<&'scope mut &'scope ()>,
}

impl<'scope, T> ScopedJoinHandle<'scope, T> {
    pub fn join(self) -> Result<T, JoinError> {
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
        let _ = self.num_active_workers.fetch_add(1, Ordering::Relaxed);
        // TODO: panic when too many workers are active!
    }
    fn finish_worker(&self, unwinding: bool) {
        if unwinding {
            self.fail_flag.store(true, Ordering::Relaxed);
        }
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
    pub fn send<T: 'scope + Send>(
        &mut self,
        worker: &'scope mut Worker,
        task: impl 'scope + Send + FnOnce() -> T,
    ) -> ScopedJoinHandle<'scope, T> {
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
