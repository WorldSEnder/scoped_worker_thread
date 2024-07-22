use std::{
    any::Any,
    cell::UnsafeCell,
    mem::ManuallyDrop,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc,
    },
    thread::{spawn, JoinHandle},
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
        // TODO: signal scope
    };

    Packet {
        result: ResultStorage::new(),
        work: UnsafeCell::new(ManuallyDrop::new(task)),
    }
}

impl<T, F> Packet<T, F> {
    pub fn new(f: F) -> Self {
        Self {
            result: ResultStorage::new(),
            work: UnsafeCell::new(ManuallyDrop::new(f)),
        }
    }
}

unsafe impl<T: Send, F: ?Sized> Send for Packet<T, F> {}
unsafe impl<T: Send, F: ?Sized> Sync for Packet<T, F> {}

impl<T, F: FnOnce(&ResultStorage<T>)> UnitOfWork for Packet<T, F> {
    unsafe fn work(&self) {
        // SAFETY: the worker thread is the only place to access the work.
        let work = unsafe { &mut *self.work.get() };
        // SAFETY: this function gets only called once. Hence, the value is populated when this gets called from initialization.
        let work = ManuallyDrop::take(work);
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
    fn recv(&mut self) -> T {
        self.recv.recv().expect("host should be present")
    }
    // SAFETY: must drop the previously received item before calling
    unsafe fn done(&mut self) {
        self.done.send(()).expect("host should be present")
    }
}

#[derive(Debug)]
pub enum JoinError {
    UnwindError(Box<dyn Any + Send + 'static>),
}

pub struct ClaimedWorker<'scope, T> {
    barrier: &'scope Receiver<()>,
    packet: SharedPacket<'scope, T>,
}

impl<'scope, T> ClaimedWorker<'scope, T> {
    pub fn join(self) -> Result<T, JoinError> {
        self.barrier.recv().expect("Worker exited unexpectedly");
        self.packet.result.claim().map_err(JoinError::UnwindError)
    }
}

pub struct Worker {
    thread: ManuallyDrop<JoinHandle<()>>,
    control: Controller<WorkItem>,
}

impl Worker {
    pub fn spawn() -> Self {
        let (send, recv) = sync_channel(1);
        let (send_done, recv_done) = sync_channel(1);
        let control = Controller {
            send,
            done: recv_done,
        };
        let mut remote = Remote {
            recv,
            done: send_done,
        };

        let worker = spawn(move || loop {
            match remote.recv() {
                WorkItem::Task(task) => {
                    // SAFETY: work has not been called before
                    unsafe { task.work() }
                }
                WorkItem::Terminate => break,
            }
            // SAFETY: received item has been dropped
            unsafe { remote.done() }
        });
        Self {
            thread: ManuallyDrop::new(worker),
            control,
        }
    }

    pub fn submit<'scope, T, F>(&'scope mut self, packet: Packet<T, F>) -> ClaimedWorker<'scope, T>
    where
        Packet<T, F>: UnitOfWork,
        F: 'scope + Send,
        T: 'scope + Send,
    {
        let our_packet = Arc::new(packet);
        let their_packet: Arc<dyn UnitOfWork + Send + Sync> = our_packet.clone();
        // SAFETY: we prolong the lifetime of the borrow
        let their_packet = unsafe { Arc::from_raw(Arc::into_raw(their_packet) as *mut _) };
        let item = WorkItem::Task(their_packet);
        let barrier = self.control.send(item);
        ClaimedWorker {
            barrier,
            packet: our_packet,
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _barrier = self.control.send(WorkItem::Terminate);
        let _ = unsafe { ManuallyDrop::take(&mut self.thread) }.join();
    }
}
