# event-listener

## Example

```rust
use std::sync;
use std::sync::atomic;
use std::thread;
use std::time;
fn main() {
    smol::block_on(async {
        let flag = sync::Arc::new(atomic::AtomicBool::new(false));
        let event = sync::Arc::new(event_listener::Event::new());
        thread::spawn({
            let flag = flag.clone();
            let event = event.clone();
            move || {
                thread::sleep(time::Duration::from_secs(3));
                flag.store(true, atomic::Ordering::SeqCst);
                event.notify(usize::MAX, false);
            }
        });
        loop {
            if flag.load(atomic::Ordering::SeqCst) {
                break;
            }
            let listener = event.listen();
            if flag.load(atomic::Ordering::SeqCst) {
                break;
            }
            listener.await;
            println!("flag is set");
        }
    });
}
```
