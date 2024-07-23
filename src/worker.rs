use std::{
    cell::UnsafeCell,
    mem::ManuallyDrop,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc,
    },
    thread::JoinHandle,
};

// A trait that is morally equivalent to `FnOnce()` that we can implement for our own structures.
pub trait UnitOfWork {
    // SAFETY: This must only be called at most once per unit of work.
    // If we had owning pointers, we would use those.
    unsafe fn work(&self);
}

enum WorkItem {
    Task(Arc<dyn 'static + Sync + Send + UnitOfWork>),
    Terminate,
}

impl Default for WorkItem {
    fn default() -> Self {
        Self::Terminate
    }
}

pub struct ResultStorage<T> {
    result: UnsafeCell<Option<std::thread::Result<T>>>,
}

impl<T> ResultStorage<T> {
    fn new() -> Self {
        Self {
            result: UnsafeCell::default(),
        }
    }
    fn store(&self, result: std::thread::Result<T>) {
        // SAFETY: only called from the worker thread, before it signals the main thread that is is finished
        unsafe {
            *self.result.get() = Some(result);
        }
    }
    fn claim(&self) -> std::thread::Result<T> {
        // SAFETY: only called on the master thread when joining. We first wait for the worker to store a result.
        unsafe { (*self.result.get()).take() }.expect("should have a stored result")
    }
}

pub struct Packet<T, F: ?Sized> {
    result: ResultStorage<T>,
    // TODO: refer to https://github.com/rust-lang/rfcs/blob/master/text/3336-maybe-dangling.md we might have to use MaybeDangling instead of ManuallyDrop
    work: UnsafeCell<ManuallyDrop<F>>,
}

pub fn new_packet<T>(
    task: impl FnOnce() -> T,
    synchronize_result: impl Fn(bool),
) -> Packet<T, impl FnOnce(&ResultStorage<T>)> {
    let task = move |storage: &ResultStorage<T>| {
        let result = catch_unwind(AssertUnwindSafe(task));
        let unwinding = result.is_err();
        storage.store(result);
        synchronize_result(unwinding);
    };

    Packet {
        result: ResultStorage::new(),
        work: UnsafeCell::new(ManuallyDrop::new(task)),
    }
}

// SAFETY: The packet is only read on two threads: the worker thread and the main thread. Borrows do not overlap, the shared ref is only used to Send data
// between these. Hence, both the return value type `T` and the closure `F` need to be `Send` but not `Sync`.
unsafe impl<T: Send, F: ?Sized + Send> Send for Packet<T, F> {}
// SAFETY: See reasoning on Send.
unsafe impl<T: Send, F: ?Sized + Send> Sync for Packet<T, F> {}

impl<T, F: FnOnce(&ResultStorage<T>)> UnitOfWork for Packet<T, F> {
    unsafe fn work(&self) {
        // SAFETY: the worker thread is the only place to access the work after construction.
        let work = unsafe { &mut *self.work.get() };
        // SAFETY: this function gets only called once. Hence, the value is populated when this gets called from initialization.
        let work = unsafe { ManuallyDrop::take(work) };
        work(&self.result);
    }
}

trait Erased {}
impl<T: ?Sized> Erased for T {}
// This type is shared between the JoinHandle and the worker.
// We use an Arc to dispose of it, because it is not clear which side drops te packet first:
// - If the join handle is joined, the worker is done first and drops its access to the packet, freeing the main thread
//   to read the result.
// - If the join handle is dropped without joining before the worker finishes its piece of work.
//   In this case, the worker is responsible for cleaning up. Note that we could also register packets in the scope
//   and clean them up on scope exit. This would save a few atomic operations and the book-keeping of Arc, but prolongs
//   the lifetime of packets until the end of scope unnecessarily.
type SharedPacket<'scope, T> = Arc<Packet<T, dyn 'scope + Erased>>;

struct Controller<T> {
    send: SyncSender<T>,
    done: Receiver<()>,
}
struct Remote<T> {
    recv: Receiver<T>,
    done: SyncSender<()>,
}
impl<T> Controller<T> {
    fn send(&mut self, item: T) -> &'_ Receiver<()> {
        self.send.send(item).expect("worker to be present");
        &self.done
    }
}
impl<T> Remote<T> {
    // Return T::default() when the host disconnects. This can happen when the worker thread gets detached.
    // In this case, we should terminate the worker "as-soon-as-possible"(TM).
    fn recv(&mut self) -> T
    where
        T: Default,
    {
        self.recv.recv().unwrap_or_default()
    }
    // SAFETY: must drop the previously received item before calling
    unsafe fn done(&mut self) {
        self.done.send(()).expect("host should be present")
    }
}
fn sync_remote_control<T>() -> (Controller<T>, Remote<T>) {
    let (send, recv) = sync_channel(1);
    let (send_done, recv_done) = sync_channel(1);
    (
        Controller {
            send,
            done: recv_done,
        },
        Remote {
            recv,
            done: send_done,
        },
    )
}
fn work_loop(mut remote: Remote<WorkItem>) -> impl FnOnce() {
    move || loop {
        match remote.recv() {
            WorkItem::Task(task) => {
                // SAFETY: work has not been called before
                unsafe { task.work() }
            }
            WorkItem::Terminate => break,
        }
        // SAFETY: received item has been dropped
        unsafe { remote.done() }
    }
}

pub struct ClaimedWorker<'scope, T> {
    barrier: &'scope Receiver<()>,
    packet: SharedPacket<'scope, T>,
}

impl<'scope, T> ClaimedWorker<'scope, T> {
    pub fn join(self) -> std::thread::Result<T> {
        self.barrier.recv().expect("Worker exited unexpectedly");
        self.packet.result.claim()
    }
}

// LET ME WRITE std::thread::ScopedJoinHandle<'static>, I don't care about the variants.
enum WorkerHandle<'thread> {
    Static(JoinHandle<()>), // In this case, 'thread = 'static
    Scoped(std::thread::ScopedJoinHandle<'thread, ()>),
}

/// A thread dedicated to performing work sent in a [`scope`](crate::scope).
pub struct Worker<'thread> {
    thread: WorkerHandle<'thread>,
    control: Controller<WorkItem>,
}

impl Worker<'static> {
    /// Spawn a new worker with default settings.
    ///
    /// Like [`Thread`](std::thread::Thread), the [`Worker`] is implicitly detached when it is dropped without joining it.
    /// Despite this, some care is taken to trigger the worker-thread to clean up after itself, but no synchronization with this clean-up is performed.
    pub fn spawn() -> Self {
        Self::with_builder(std::thread::Builder::new()).unwrap()
    }
    /// Spawn a new work from a given thread builder.
    pub fn with_builder(builder: std::thread::Builder) -> std::io::Result<Self> {
        let (control, remote) = sync_remote_control();
        let handle = builder.spawn(work_loop(remote))?;
        Ok(Self {
            thread: WorkerHandle::Static(handle),
            control,
        })
    }
}
impl<'thread> Worker<'thread> {
    /// Spawn a new worker with default settings in a scope.
    pub fn spawn_scoped(scope: &'thread std::thread::Scope<'thread, '_>) -> Self {
        Self::with_builder_scoped(std::thread::Builder::new(), scope).unwrap()
    }
    /// Spawn a new worker from a given thread builder in a scope.
    pub fn with_builder_scoped(
        builder: std::thread::Builder,
        scope: &'thread std::thread::Scope<'thread, '_>,
    ) -> std::io::Result<Self> {
        let (control, remote) = sync_remote_control();
        let handle = builder.spawn_scoped(scope, work_loop(remote))?;
        Ok(Self {
            thread: WorkerHandle::Scoped(handle),
            control,
        })
    }
    /// Join a worker and synchronize with the thread termination.
    pub fn join(self) {
        // Note the order: we first send the termination signal, then manually run clean-up.
        let Self { control, thread } = self;
        // dropping our control will cause the worker to recv Terminate
        drop(control);
        match thread {
            WorkerHandle::Static(handle) => handle.join().unwrap(),
            WorkerHandle::Scoped(handle) => handle.join().unwrap(),
        }
    }
    /// Submit a unit of work to the worker and claim it.
    pub(crate) fn submit<'scope, T, F>(
        &'scope mut self,
        packet: Packet<T, F>,
    ) -> ClaimedWorker<'scope, T>
    where
        Packet<T, F>: UnitOfWork,
        F: 'scope + Send,
        T: 'scope + Send,
    {
        let our_packet = Arc::new(packet);
        let their_packet: Arc<dyn UnitOfWork + Send + Sync> = our_packet.clone();
        // SAFETY: we merely prolong the lifetime of the borrow. This lifetime is threaded into the ClaimedWorker struct.
        let their_packet = unsafe { Arc::from_raw(Arc::into_raw(their_packet) as *mut _) };
        let work = WorkItem::Task(their_packet);
        let barrier = self.control.send(work);
        ClaimedWorker {
            barrier,
            packet: our_packet,
        }
    }
}
