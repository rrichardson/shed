//! a collection of streams which can be mutated after creation time in a thread-safe way
//! This was directly copied from futures::stream::select_all
//! At the moment it's pretty much the same as the original SelectAll impl, except that it features
//! a mutex in order for the high-level future to be able to accept pushes from any thread at any
//! time.  It might diverge more in the future

use core::fmt::{self, Debug};
use core::iter::FromIterator;
use core::pin::Pin;
use futures::ready;
use futures::stream::{FusedStream, Stream};
use futures::task::{Context, Poll};
use parking_lot::Mutex;
use std::sync::Arc;

use crate::stream::{FuturesUnordered, StreamExt, StreamFuture};

/// An unbounded set of streams
///
/// This "combinator" provides the ability to maintain a set of streams
/// and drive them all to completion.
///
/// Streams are pushed into this set and their realized values are
/// yielded as they become ready. Streams will only be polled when they
/// generate notifications. This allows to coordinate a large number of streams.
///
/// Note that you can create a ready-made `Manifold` via the
/// `select_all` function in the `stream` module, or you can start with an
/// empty set with the `Manifold::new` constructor.
#[must_use = "streams do nothing unless polled"]
pub struct Manifold<St> {
    inner: Arc<Mutex<FuturesUnordered<StreamFuture<St>>>>,
}

impl<St> Clone for Manifold<St> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

impl<St: Debug> Debug for Manifold<St> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Manifold {{ ... }}")
    }
}

impl<St: Stream + Unpin> Manifold<St> {
    /// Constructs a new, empty `Manifold`
    ///
    /// The returned `Manifold` does not contain any streams and, in this
    /// state, `Manifold::poll` will return `Poll::Ready(None)`.
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(FuturesUnordered::new())) }
    }

    /// Returns the number of streams contained in the set.
    ///
    /// This represents the total number of in-flight streams.
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Returns `true` if the set contains no streams
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// Push a stream into the set.
    ///
    /// This function submits the given stream to the set for managing. This
    /// function will not call `poll` on the submitted stream. The caller must
    /// ensure that `Manifold::poll` is called in order to receive task
    /// notifications.
    pub fn push(&self, stream: St) {
        self.inner.lock().push(stream.into_future());
    }
}

impl<St: Stream + Unpin> Default for Manifold<St> {
    fn default() -> Self {
        Self::new()
    }
}

impl<St: Stream + Unpin> Stream for Manifold<St> {
    type Item = St::Item;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.lock();
        loop {
            match ready!(inner.poll_next_unpin(cx)) {
                Some((Some(item), remaining)) => {
                    inner.push(remaining.into_future());
                    return Poll::Ready(Some(item));
                }
                Some((None, _)) => {
                    // `FuturesUnordered` thinks it isn't terminated
                    // because it yielded a Some.
                    // We do not return, but poll `FuturesUnordered`
                    // in the next loop iteration.
                }
                None => return Poll::Ready(None),
            }
        }
    }
}

impl<St: Stream + Unpin> FusedStream for Manifold<St> {
    fn is_terminated(&self) -> bool {
        self.inner.lock().is_terminated()
    }
}

/// Convert a list of streams into a `Stream` of results from the streams.
///
/// This essentially takes a list of streams (e.g. a vector, an iterator, etc.)
/// and bundles them together into a single stream.
/// The stream will yield items as they become available on the underlying
/// streams internally, in the order they become available.
///
/// Note that the returned set can also be used to dynamically push more
/// futures into the set as they become available.
///
/// This function is only available when the `std` or `alloc` feature of this
/// library is activated, and it is activated by default.
pub fn manifold<I>(streams: I) -> Manifold<I::Item>
where
    I: IntoIterator,
    I::Item: Stream + Unpin,
{
    let set = Manifold::new();

    for stream in streams {
        set.push(stream);
    }

    assert_stream::<<I::Item as Stream>::Item, _>(set)
}

impl<St: Stream + Unpin> FromIterator<St> for Manifold<St> {
    fn from_iter<T: IntoIterator<Item = St>>(iter: T) -> Self {
        manifold(iter)
    }
}

impl<St: Stream + Unpin> Extend<St> for Manifold<St> {
    fn extend<T: IntoIterator<Item = St>>(&mut self, iter: T) {
        for st in iter {
            self.push(st)
        }
    }
}

pub(crate) fn assert_stream<T, S>(stream: S) -> S
where
    S: Stream<Item = T>,
{
    stream
}
