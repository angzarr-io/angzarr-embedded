//! An order-preserving parallel map over scoped threads.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Map `f` over `items` on every core, keeping the order: `out[i]` is
/// `f(&items[i])`. Work is handed out one item at a time, so uneven items
/// balance. With deterministic `f` the result is deterministic.
///
/// A panic in `f` propagates once every thread has stopped.
pub fn parallel<T: Sync, R: Send>(items: &[T], f: impl Fn(&T) -> R + Sync) -> Vec<R> {
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(items.len().max(1));
    let next = AtomicUsize::new(0);
    let slots: Vec<Mutex<Option<R>>> = items.iter().map(|_| Mutex::new(None)).collect();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= items.len() {
                    break;
                }
                let r = f(&items[i]);
                *slots[i].lock().unwrap_or_else(|e| e.into_inner()) = Some(r);
            });
        }
    });
    slots
        .into_iter()
        .map(|s| {
            s.into_inner()
                .unwrap_or_else(|e| e.into_inner())
                .expect("every slot is filled once the scope ends")
        })
        .collect()
}

#[cfg(test)]
#[path = "parallel.test.rs"]
mod tests;
