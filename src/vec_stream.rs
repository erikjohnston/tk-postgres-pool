
use std::collections::VecDeque;
use std::mem;
use std::sync::{Arc, Mutex};
use futures::{Async, Poll, Stream, task};
use futures::task::Task;

struct VecStreamInner<T, E> {
    task: Option<Task>,
    state: State<T, E>,
}

impl<T, E> VecStreamInner<T, E> {
    fn new() -> VecStreamInner<T, E> {
        VecStreamInner {
            task: None,
            state: State::Items {
                queue: VecDeque::new(),
            }
        }
    }
}

enum State<T, E> {
    Empty,
    Done,
    Items { queue: VecDeque<T> },
    Errored { error: E },
}

pub struct VecStreamReceiver<T, E> {
    inner: Arc<Mutex<VecStreamInner<T, E>>>,
}

impl<T, E> Stream for VecStreamReceiver<T, E> {
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Poll<Option<T>, E> {
        let mut inner = self.inner.lock().expect("mutext was poisoned");

        let state = mem::replace(&mut inner.state, State::Empty);

        match state {
            State::Empty => panic!("Cannot call stream twice"),
            State::Done => Ok(Async::Ready(None)),
            State::Errored { error } => Err(error),
            State::Items { mut queue } => {
                let front = queue.pop_front();
                inner.state = State::Items { queue: queue };
                
                if let Some(item) = front {
                    Ok(Async::Ready(Some(item)))
                } else {
                    if inner.task.is_none() {
                        inner.task = Some(task::park());
                    }
                    Ok(Async::NotReady)
                }
            }
        }
    }
}

impl<T, E> Drop for VecStreamReceiver<T, E> {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.state = State::Done;
        }
    }
}


pub struct VecStreamSender<T, E> {
    inner: Arc<Mutex<VecStreamInner<T, E>>>,
}

impl<T, E> VecStreamSender<T, E> {
    pub fn send(&mut self, item: T) {
        let mut inner = self.inner.lock().expect("mutext was poisoned");

        if let State::Items { ref mut queue } = inner.state {
            queue.push_back(item);
        }
    }

    pub fn error(&mut self, error: E) {
        let mut inner = self.inner.lock().expect("mutext was poisoned");

        match inner.state {
            State::Items { .. } => inner.state = State::Errored { error: error },
            _ => {}
        }
    }

    pub fn close(&mut self) {
        let mut inner = self.inner.lock().expect("mutext was poisoned");

        match inner.state {
            State::Items { .. } => inner.state = State::Done,
            _ => {}
        }
    }
}


pub fn create_stream<T, E>() -> (VecStreamSender<T, E>, VecStreamReceiver<T,E>) {
    let inner = Arc::new(Mutex::new(VecStreamInner::new()));

    let sender = VecStreamSender {
        inner: inner.clone(),
    };

    let receiver = VecStreamReceiver {
        inner: inner,
    };

    (sender, receiver)
}


#[cfg(test)]
mod tests {
    use super::*;
    use futures;
    use futures::{Future, Stream};

    #[test]
    fn normal() {
        let (mut sender, mut receiver) = create_stream::<u8, ()>();

        futures::lazy(move || -> Result<(), ()> {
            assert_eq!(receiver.poll(), Ok(Async::NotReady));

            sender.send(0);

            assert_eq!(receiver.poll(), Ok(Async::Ready(Some(0))));
            assert_eq!(receiver.poll(), Ok(Async::NotReady));

            sender.send(1);

            assert_eq!(receiver.poll(), Ok(Async::Ready(Some(1))));
            assert_eq!(receiver.poll(), Ok(Async::NotReady));

            sender.close();

            assert_eq!(receiver.poll(), Ok(Async::Ready(None)));

            Ok(())
        }).wait().unwrap();
    }

    #[test]
    fn error() {
        let (mut sender, mut receiver) = create_stream::<u8, ()>();

        futures::lazy(move || -> Result<(), ()> {
            assert_eq!(receiver.poll(), Ok(Async::NotReady));

            sender.send(0);

            assert_eq!(receiver.poll(), Ok(Async::Ready(Some(0))));
            assert_eq!(receiver.poll(), Ok(Async::NotReady));

            sender.send(1);

            assert_eq!(receiver.poll(), Ok(Async::Ready(Some(1))));
            assert_eq!(receiver.poll(), Ok(Async::NotReady));

            sender.error(());

            assert_eq!(receiver.poll(), Err(()));

            Ok(())
        }).wait().unwrap();
    }
}