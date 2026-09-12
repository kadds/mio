//! Mio selector backed by the NaOS capability epoll interface.

use naos_sys as sys;

use crate::{Interest, Token};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const WAKE_DATA: u64 = u64::MAX;

#[derive(Clone, Copy, Debug)]
struct Registration {
    handle: sys::Handle,
    token: Token,
    interests: Interest,
}

#[derive(Debug)]
struct Shared {
    registrations: Mutex<Vec<Registration>>,
    wake_token: Mutex<Token>,
    registration_wake: AtomicBool,
    external_wake: AtomicBool,
    epoll: sys::Handle,
    wake_receiver: sys::Handle,
    wake_sender: sys::Handle,
}

impl Shared {
    fn send_wake(&self) -> io::Result<()> {
        let frame = sys::ChannelSendFrame {
            struct_size: core::mem::size_of::<sys::ChannelSendFrame>() as u32,
            ..sys::ChannelSendFrame::default()
        };

        // A wakeup is level-triggered by the queued empty message.  If one is
        // already queued, the selector is already guaranteed to return.
        let status = unsafe { sys::_na_channel_send(self.wake_sender, &frame) };
        if status == sys::STATUS_OK || status == sys::STATUS_WOULD_BLOCK {
            Ok(())
        } else {
            Err(status_error("channel wake", status))
        }
    }

    fn wake(&self) -> io::Result<()> {
        self.external_wake.store(true, Ordering::Release);
        self.send_wake()
    }

    fn wake_registration(&self) -> io::Result<()> {
        self.registration_wake.store(true, Ordering::Release);
        self.send_wake()
    }

    fn lock(&self) -> io::Result<std::sync::MutexGuard<'_, Vec<Registration>>> {
        self.registrations
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "NaOS Mio registry lock poisoned"))
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        unsafe {
            let _ = sys::_na_handle_close(self.epoll);
            let _ = sys::_na_handle_close(self.wake_receiver);
            let _ = sys::_na_handle_close(self.wake_sender);
        }
    }
}

/// NaOS selector state shared by Mio `Registry` clones.
#[derive(Clone, Debug)]
pub struct Selector {
    shared: Arc<Shared>,
}

impl Selector {
    /// Creates a selector and its private channel used for cross-thread
    /// wakeups and registration changes.
    pub fn new() -> io::Result<Self> {
        let options = sys::ChannelOptions {
            struct_size: core::mem::size_of::<sys::ChannelOptions>() as u32,
            max_messages: 1,
            max_bytes: 1,
            ..sys::ChannelOptions::default()
        };
        let mut epoll = sys::HANDLE_INVALID;
        let status = unsafe { sys::_na_epoll_create(&mut epoll) };
        if status != sys::STATUS_OK {
            return Err(status_error("epoll create", status));
        }

        let mut wake_receiver = sys::HANDLE_INVALID;
        let mut wake_sender = sys::HANDLE_INVALID;
        let status =
            unsafe { sys::_na_channel_create(&options, &mut wake_receiver, &mut wake_sender) };
        if status != sys::STATUS_OK {
            unsafe { sys::_na_handle_close(epoll) };
            return Err(status_error("channel create", status));
        }

        let wake_event = sys::EpollEvent {
            events: sys::EPOLL_EVENT_READABLE,
            data: WAKE_DATA,
        };
        let status = unsafe {
            sys::_na_epoll_ctl(
                epoll,
                sys::EPOLL_CTL_ADD,
                wake_receiver,
                &wake_event,
            )
        };
        if status != sys::STATUS_OK {
            unsafe {
                sys::_na_handle_close(epoll);
                sys::_na_handle_close(wake_receiver);
                sys::_na_handle_close(wake_sender);
            }
            return Err(status_error("epoll add wake channel", status));
        }

        Ok(Self {
            shared: Arc::new(Shared {
                registrations: Mutex::new(Vec::new()),
                wake_token: Mutex::new(Token(0)),
                registration_wake: AtomicBool::new(false),
                external_wake: AtomicBool::new(false),
                epoll,
                wake_receiver,
                wake_sender,
            }),
        })
    }

    /// Clones a registry view onto the same selector.
    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(self.clone())
    }

    /// Waits for readiness on the kernel-owned epoll queue.
    pub fn select(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        let mut deadline = timeout.map(deadline_after);
        let capacity = events.capacity().max(1);
        let mut returned = vec![sys::EpollEvent::default(); capacity];
        loop {
            let deadline_ptr = deadline.as_mut().map_or(core::ptr::null(), |value| {
                value as *const sys::TimeClock as *const u8
            });
            let mut actual = 0;
            let status = unsafe {
                sys::_na_epoll_wait(
                    self.shared.epoll,
                    returned.as_mut_ptr(),
                    returned.len() as u64,
                    &mut actual,
                    deadline_ptr,
                )
            };

            events.clear();
            if status == sys::STATUS_WAIT_TIMED_OUT || status == sys::STATUS_WOULD_BLOCK {
                return Ok(());
            }
            if status != sys::STATUS_OK {
                return Err(status_error("epoll wait", status));
            }

            let actual = (actual as usize).min(returned.len());
            let mut saw_wake = false;
            for event in &returned[..actual] {
                if event.data != WAKE_DATA {
                    let closed = event.events & sys::EPOLL_EVENT_HANGUP != 0;
                    events.push(Event::new(
                        Token(event.data as usize),
                        event.events & sys::EPOLL_EVENT_READABLE != 0 || closed,
                        event.events & sys::EPOLL_EVENT_WRITABLE != 0 || closed,
                        event.events & sys::EPOLL_EVENT_ERROR != 0,
                        closed,
                        closed,
                    ));
                    if events.len() == events.capacity() {
                        break;
                    }
                    continue;
                }
                saw_wake = true;
                drain_wake_channel(self.shared.wake_receiver)?;
            }

            if saw_wake {
                let registration_wake = self.shared.registration_wake.swap(false, Ordering::Acquire);
                let external_wake = self.shared.external_wake.swap(false, Ordering::Acquire);
                if events.is_empty() && registration_wake && !external_wake {
                    // A source registration changed synchronously on the
                    // selector thread. Its wake only interrupts the current
                    // epoll wait; it must not make Tokio stop waiting before
                    // the newly registered capability can report readiness.
                    continue;
                }
                if external_wake && events.capacity() != 0 {
                    let token = *self.shared.wake_token.lock().map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "NaOS Mio waker lock poisoned")
                    })?;
                    events.push(Event::new(token, true, false, false, false, false));
                }
            }
            return Ok(());
        }
    }

    /// Registers a capability handle with readiness interests.
    pub fn register(
        &self,
        handle: sys::Handle,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        let mut registrations = self.shared.lock()?;
        if registrations.iter().any(|registration| registration.handle == handle) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "NaOS handle already registered",
            ));
        }
        let event = sys::EpollEvent {
            events: interests_to_epoll_events(interests),
            data: token.0 as u64,
        };
        let status = unsafe {
            sys::_na_epoll_ctl(self.shared.epoll, sys::EPOLL_CTL_ADD, handle, &event)
        };
        if status != sys::STATUS_OK {
            return Err(status_error("epoll add", status));
        }
        registrations.push(Registration {
            handle,
            token,
            interests,
        });
        drop(registrations);
        self.shared.wake_registration()
    }

    /// Updates a registered handle.
    pub fn reregister(
        &self,
        handle: sys::Handle,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        let mut registrations = self.shared.lock()?;
        let Some(registration) = registrations
            .iter_mut()
            .find(|registration| registration.handle == handle)
        else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "NaOS handle is not registered",
            ));
        };
        let event = sys::EpollEvent {
            events: interests_to_epoll_events(interests),
            data: token.0 as u64,
        };
        let status = unsafe {
            sys::_na_epoll_ctl(self.shared.epoll, sys::EPOLL_CTL_MOD, handle, &event)
        };
        if status != sys::STATUS_OK {
            return Err(status_error("epoll modify", status));
        }
        registration.token = token;
        registration.interests = interests;
        drop(registrations);
        self.shared.wake_registration()
    }

    /// Removes a registered handle.
    pub fn deregister(&self, handle: sys::Handle) -> io::Result<()> {
        let mut registrations = self.shared.lock()?;
        let Some(index) = registrations
            .iter()
            .position(|registration| registration.handle == handle)
        else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "NaOS handle is not registered",
            ));
        };
        let status = unsafe {
            sys::_na_epoll_ctl(
                self.shared.epoll,
                sys::EPOLL_CTL_DEL,
                handle,
                core::ptr::null(),
            )
        };
        if status != sys::STATUS_OK {
            return Err(status_error("epoll delete", status));
        }
        registrations.swap_remove(index);
        drop(registrations);
        self.shared.wake_registration()
    }
}

/// A readiness event returned by the NaOS selector.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Event {
    token: Token,
    readable: bool,
    writable: bool,
    error: bool,
    read_closed: bool,
    write_closed: bool,
}

impl Event {
    fn new(
        token: Token,
        readable: bool,
        writable: bool,
        error: bool,
        read_closed: bool,
        write_closed: bool,
    ) -> Self {
        Self {
            token,
            readable,
            writable,
            error,
            read_closed,
            write_closed,
        }
    }
}

/// Collection of events returned by the NaOS selector.
pub type Events = Vec<Event>;

pub mod event {
    use super::Event;
    use crate::Token;
    use std::fmt;

    pub fn token(event: &Event) -> Token {
        event.token
    }

    pub fn is_readable(event: &Event) -> bool {
        event.readable
    }

    pub fn is_writable(event: &Event) -> bool {
        event.writable
    }

    pub fn is_error(event: &Event) -> bool {
        event.error
    }

    pub fn is_read_closed(event: &Event) -> bool {
        event.read_closed
    }

    pub fn is_write_closed(event: &Event) -> bool {
        event.write_closed
    }

    pub fn is_priority(_: &Event) -> bool {
        false
    }

    pub fn is_aio(_: &Event) -> bool {
        false
    }

    pub fn is_lio(_: &Event) -> bool {
        false
    }

    pub fn debug_details(f: &mut fmt::Formatter<'_>, event: &Event) -> fmt::Result {
        f.debug_struct("NaosEvent")
            .field("token", &event.token)
            .field("readable", &event.readable)
            .field("writable", &event.writable)
            .field("error", &event.error)
            .field("read_closed", &event.read_closed)
            .field("write_closed", &event.write_closed)
            .finish()
    }
}

/// Waker implementation backed by a NaOS channel endpoint.
#[derive(Debug)]
pub struct Waker {
    shared: Arc<Shared>,
    token: Token,
}

impl Waker {
    pub fn new(selector: &Selector, token: Token) -> io::Result<Self> {
        *selector
            .shared
            .wake_token
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "NaOS Mio waker lock poisoned"))? =
            token;
        Ok(Self {
            shared: Arc::clone(&selector.shared),
            token,
        })
    }

    pub fn wake(&self) -> io::Result<()> {
        let _ = self.token;
        self.shared.wake()
    }
}

fn interests_to_epoll_events(interests: Interest) -> u64 {
    // Tokio's readiness cache advances its tick for every selector event.
    // Sources must therefore be edge-triggered and drained to WouldBlock;
    // level-triggering the same unread capability can continually advance the
    // tick before `clear_readiness()` runs and leave stale cached readiness.
    let mut events = 0;
    if interests.is_readable() {
        events |= sys::EPOLL_EVENT_READABLE;
    }
    if interests.is_writable() {
        events |= sys::EPOLL_EVENT_WRITABLE;
    }
    events | sys::EPOLL_EVENT_EDGE_TRIGGERED
}

fn drain_wake_channel(handle: sys::Handle) -> io::Result<()> {
    loop {
        let mut frame = sys::ChannelReceiveFrame {
            struct_size: core::mem::size_of::<sys::ChannelReceiveFrame>() as u32,
            ..sys::ChannelReceiveFrame::default()
        };
        let status = unsafe { sys::_na_channel_receive(handle, &mut frame) };
        if status == sys::STATUS_WOULD_BLOCK {
            return Ok(());
        }
        if status != sys::STATUS_OK {
            return Err(status_error("channel drain", status));
        }
    }
}

fn deadline_after(timeout: Duration) -> sys::TimeClock {
    let mut now = sys::TimeClock::default();
    let status = unsafe { sys::_s_clock(1, &mut now) };
    if status != 0 || now.tv_sec < 0 || now.tv_nsec < 0 {
        return sys::TimeClock {
            tv_sec: i64::MAX,
            tv_nsec: 999_999_999,
        };
    }

    let timeout_secs = timeout.as_secs().min(i64::MAX as u64) as i64;
    let timeout_nanos = i64::from(timeout.subsec_nanos());
    let mut seconds = now.tv_sec.saturating_add(timeout_secs);
    let mut nanos = now.tv_nsec.saturating_add(timeout_nanos);
    if nanos >= 1_000_000_000 {
        seconds = seconds.saturating_add(1);
        nanos -= 1_000_000_000;
    }
    sys::TimeClock {
        tv_sec: seconds,
        tv_nsec: nanos,
    }
}

fn status_error(operation: &'static str, status: sys::Status) -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        format!("{operation}: NaOS status {status}"),
    )
}
