use super::*;

#[test]
fn example() {
    let mut worker1 = Worker::spawn();
    let mut worker2 = Worker::spawn();
    let mut result1 = 0;
    let mut result2 = 0;

    let hostid = std::thread::current().id();
    scope(|scope| {
        scope.send(&mut worker1, || {
            assert_ne!(std::thread::current().id(), hostid);
            result1 = 42;
        });
        scope.send(&mut worker2, || {
            assert_ne!(std::thread::current().id(), hostid);
            result2 = 42;
        });
    });
    assert!(result1 == 42);
    assert!(result2 == 42);
    worker1.join();
    worker2.join();
}
