use std::pin;
use std::{
    cell::{self, RefCell},
    rc, sync, task,
};
/// [event_listener::Event]
pub struct Event {
    listeners: sync::Arc<sync::Mutex<LinkedStateList>>,
}
impl Event {
    /// [event_listener::Event::new]
    pub fn new() -> Self {
        Self {
            listeners: sync::Arc::new(sync::Mutex::new(LinkedStateList {
                head: None,
                tail: None,
                next: None,
                len: 0,
                notified: 0,
            })),
        }
    }
    /// [event_listener::Event::total_listeners]
    pub fn total_listeners(&self) -> usize {
        self.listeners.lock().unwrap().len
    }
    /// [event_listener::Event::listen]
    pub fn listen(&self) -> EventListener {
        let listener = rc::Rc::new(RefCell::new(LinkedState {
            state: cell::Cell::new(State::Created),
            prev: None,
            next: None,
        }));
        let mut listeners = self.listeners.lock().unwrap();
        match &listeners.tail {
            Some(tail) => {
                tail.borrow_mut().next = Some(listener.clone());
                listener.borrow_mut().prev = Some(tail.clone());
                listeners.tail = Some(listener.clone());
            }
            None => {
                listeners.head = Some(listener.clone());
                listeners.tail = Some(listener.clone());
            }
        }
        if listeners.next.is_none() {
            listeners.next = listeners.tail.clone();
        }
        listeners.len += 1;
        EventListener {
            listeners: sync::Arc::clone(&self.listeners),
            listener: listener.clone(),
        }
    }
    /// [event_listener::Event::notify]
    pub fn notify(&self, mut n: usize, additional: bool) -> usize {
        let mut listeners = self.listeners.lock().unwrap();
        if !additional {
            if n <= listeners.notified {
                return 0;
            }
            n -= listeners.notified;
        }
        let mut remaining = n;
        while remaining > 0 {
            let next = match &listeners.next {
                Some(next) => next.clone(),
                None => {
                    return n - remaining;
                }
            };
            listeners.next = next.borrow().next.clone();
            if let State::Waiting(waker) = next.borrow().state.replace(State::Notified) {
                waker.wake();
            };
            listeners.notified += 1;
            remaining -= 1;
        }
        n - remaining
    }
}
/// [event_listener::EventListener]
pub struct EventListener {
    listeners: sync::Arc<sync::Mutex<LinkedStateList>>,
    listener: rc::Rc<RefCell<LinkedState>>,
}
impl Future for EventListener {
    type Output = ();
    fn poll(self: pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let this = self.get_mut();
        match this.listener.borrow().state.replace(State::Created) {
            State::Notified => {
                this.listener.borrow().state.set(State::Notified);
                task::Poll::Ready(())
            }
            State::Waiting(other) => {
                this.listener.borrow().state.set(State::Waiting({
                    if !cx.waker().will_wake(&other) {
                        cx.waker().clone()
                    } else {
                        other
                    }
                }));
                task::Poll::Pending
            }
            _ => {
                this.listener
                    .borrow()
                    .state
                    .set(State::Waiting(cx.waker().clone()));
                task::Poll::Pending
            }
        }
    }
}
impl Drop for EventListener {
    fn drop(&mut self) {
        let mut listeners = self.listeners.lock().unwrap();
        let listener = self.listener.clone();
        let prev = listener.borrow().prev.clone();
        let next = listener.borrow().next.clone();
        match &prev {
            Some(prev) => prev.borrow_mut().next = next.clone(),
            None => listeners.head = next.clone(),
        }
        match &next {
            Some(next) => next.borrow_mut().prev = prev,
            None => listeners.tail = prev,
        }
        if listeners
            .next
            .as_ref()
            .map(|rc| rc::Rc::ptr_eq(rc, &listener))
            .unwrap_or(false)
        {
            listeners.next = next;
        }
        let state = listener.borrow().state.replace(State::Created);
        let notified = matches!(state, State::Notified);
        listeners.len -= 1;
        if notified {
            listeners.notified -= 1;
        }
    }
}
struct LinkedStateList {
    len: usize,
    notified: usize,
    head: Option<rc::Rc<RefCell<LinkedState>>>,
    tail: Option<rc::Rc<RefCell<LinkedState>>>,
    next: Option<rc::Rc<RefCell<LinkedState>>>,
}
unsafe impl Send for LinkedStateList {}
struct LinkedState {
    state: cell::Cell<State>,
    prev: Option<rc::Rc<RefCell<LinkedState>>>,
    next: Option<rc::Rc<RefCell<LinkedState>>>,
}
enum State {
    Created,
    Waiting(task::Waker),
    Notified,
}
