#![warn(rust_2018_idioms)]
use std::cmp::Ordering;
use std::task::Poll;

use binary_heap_plus::BinaryHeap;
use compare::Compare;
use futures::future::{join_all, JoinAll};
use futures::{ready, stream::StreamFuture, FutureExt, Stream, StreamExt};
use pin_project_lite::pin_project;

/// Head element and tail stream pair
#[derive(Debug)]
pub struct HeadTail<S>
where
    S: Stream,
{
    head: S::Item,
    tail: S,
}

pin_project! {
    /// A stream adaptor that merges an abitrary number of base streams
    /// according to an ordering function.
    ///
    /// Iterator element type is `S::Item`.
    ///
    /// See [`.kmerge_by()`](crate::kmerge_by) for more
    /// information.
    #[must_use = "stream adaptors are lazy and do nothing unless consumed"]
    pub struct KWayMergeBy<S, C>
    where
        S: Stream,
        S: Unpin,
        C: Compare<HeadTail<S>>
    {
        initial: Option<JoinAll<StreamFuture<S>>>,
        next: Option<S>,
        heap: BinaryHeap<HeadTail<S>, C>,
    }
}

/// A stream adaptor that merges an abitrary number of base streams in descending order.
/// If all base streams are sorted (descending), the result is sorted.
///
/// Stream element type is `S::Item`.
///
/// See [`.kmerge()`](crate::kmerge) for more information.
#[must_use = "stream adaptors are lazy and do nothing unless consumed"]
pub type KWayMerge<I> = KWayMergeBy<I, OrdComparator>;

pub struct OrdComparator;

impl<S> Compare<HeadTail<S>> for OrdComparator
where
    S: Stream,
    S::Item: Ord,
{
    fn compare(&self, l: &HeadTail<S>, r: &HeadTail<S>) -> std::cmp::Ordering {
        l.head.cmp(&r.head)
    }
}

pub struct FnComparator<F> {
    f: F,
}

impl<S, F> Compare<HeadTail<S>> for FnComparator<F>
where
    S: Stream,
    F: Fn(&S::Item, &S::Item) -> Ordering,
{
    fn compare(&self, l: &HeadTail<S>, r: &HeadTail<S>) -> std::cmp::Ordering {
        (self.f)(&l.head, &r.head)
    }
}

pub struct KeyComparator<F> {
    f: F,
}

impl<S, F, O> Compare<HeadTail<S>> for KeyComparator<F>
where
    S: Stream,
    F: Fn(&S::Item) -> O,
    O: Ord,
{
    fn compare(&self, l: &HeadTail<S>, r: &HeadTail<S>) -> std::cmp::Ordering {
        (self.f)(&l.head).cmp(&(self.f)(&r.head))
    }
}

/// Create a stream that merges elements of the contained streams using
/// the ordering function.
///
/// ```
/// # tokio_test::block_on(async {
/// use futures::{stream, StreamExt};
/// use stream_kmerge::kmerge;
///
/// let streams = vec![stream::iter(vec![5, 3, 1]), stream::iter(vec![4, 3, 2])];
///
/// assert_eq!(
///     kmerge(streams).collect::<Vec<usize>>().await,
///     vec![5, 4, 3, 3, 2, 1],
/// );
/// # })
/// ```
pub fn kmerge<S>(xs: impl IntoIterator<Item = S>) -> KWayMerge<S>
where
    S: Stream + Unpin,
    S::Item: Ord,
{
    assert_stream::<S::Item, _>(kmerge_generic(xs, OrdComparator))
}

/// Create a stream that merges elements of the contained streams.
///
/// ```
/// # tokio_test::block_on(async {
/// use futures::{stream, StreamExt};
/// use stream_kmerge::kmerge_by;
///
/// let streams = vec![stream::iter(vec![1, 3, 5]), stream::iter(vec![2, 3, 4])];
///
/// assert_eq!(
///     kmerge_by(streams, |x: &usize, y: &usize| y.cmp(&x)).collect::<Vec<usize>>().await,
///     vec![1, 2, 3, 3, 4, 5],
/// );
/// # })
/// ```
pub fn kmerge_by<S, F>(xs: impl IntoIterator<Item = S>, f: F) -> KWayMergeBy<S, FnComparator<F>>
where
    S: Stream + Unpin,
    F: Fn(&S::Item, &S::Item) -> Ordering,
{
    kmerge_generic(xs, FnComparator { f })
}

/// Create a stream that merges elements of the contained streams.
///
/// ```
/// # tokio_test::block_on(async {
/// use futures::{stream, StreamExt};
/// use stream_kmerge::kmerge_by_key;
///
/// let streams = vec![stream::iter(vec![("a", 5), ("a", 3)]), stream::iter(vec![("b", 4), ("b", 4)])];
///
/// assert_eq!(
///     kmerge_by_key(streams, |x: &(&'static str, usize)| x.1).collect::<Vec<_>>().await,
///     vec![("a", 5), ("b", 4), ("b", 4), ("a", 3)],
/// );
/// # })
/// ```
pub fn kmerge_by_key<S, F, O>(
    xs: impl IntoIterator<Item = S>,
    f: F,
) -> KWayMergeBy<S, KeyComparator<F>>
where
    S: Stream + Unpin,
    F: Fn(&S::Item) -> O,
    O: Ord,
{
    kmerge_generic(xs, KeyComparator { f })
}

/// This was originally meant to be [`kmerge_by`], but triggers a compiler bug, if you directly pass
/// a closure without type hint for `less_than` (https://github.com/rust-lang/rust/issues/81511).
/// More specifically, the following code fails to compile:
///
/// ```norust
/// use stream_kmerge::kmerge_generic;
/// use futures::stream;
///
/// kmerge_generic(vec![stream::empty::<usize>()], |x, y| x < y);
/// ```
///
/// If you add a type hint to the closure parameter's, it works:
///
/// ```norust
/// use stream_kmerge::kmerge_generic;
/// use futures::stream;
///
/// kmerge_generic(vec![stream::empty::<usize>()], |x: &_, y: &_| x < y);
/// ```
///
/// The error message is
/// ```norust
/// error[E0308]: mismatched types
///    --> src/lib.rs:165:1
///     |
/// 7   | kmerge_generic(vec![stream::empty::<usize>()], |x, y| x < y);
///     | ^^^^^^^^^^^^^^ lifetime mismatch
///     |
///     = note: expected type `for<'r, 's> FnMut<(&'r usize, &'s usize)>`
///                found type `FnMut<(&usize, &usize)>`
/// note: this closure does not fulfill the lifetime requirements
///    --> src/lib.rs:165:48
///     |
/// 7   | kmerge_generic(vec![stream::empty::<usize>()], |x, y| x < y);
///     |                                                ^^^^^^^^^^^^
/// note: the lifetime requirement is introduced here
///    --> /home/thomas/src/stream-kmerge/src/lib.rs:171:8
///     |
/// 171 |     F: KMergePredicate<S::Item>,
///     |        ^^^^^^^^^^^^^^^^^^^^^^^^
///
/// error: aborting due to previous error
///
/// For more information about this error, try `rustc --explain E0308`.
/// Couldn't compile the test.
/// ```
///
/// Therefore, `kmerge_generic` is private and the public [`kmerge_by`] explicitly takes a closure
/// instead of [`KMergePredicate`].
fn kmerge_generic<S, C>(xs: impl IntoIterator<Item = S>, cmp: C) -> KWayMergeBy<S, C>
where
    S: Stream + Unpin,
    C: Compare<HeadTail<S>>,
{
    let iter = xs.into_iter();
    let (min_size, _) = iter.size_hint();
    assert_stream::<S::Item, _>(KWayMergeBy {
        initial: Some(join_all(iter.map(|x| x.into_future()))),
        next: None,
        heap: BinaryHeap::from_vec_cmp(Vec::with_capacity(min_size), cmp),
    })
}

impl<S, C> Stream for KWayMergeBy<S, C>
where
    S: Stream + Unpin,
    C: Compare<HeadTail<S>>,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        if let Some(init_fut) = this.initial.as_mut() {
            let xs = ready!(init_fut.poll_unpin(cx));
            *this.initial = None;
            this.heap.extend(
                xs.into_iter().filter_map(|(head_option, tail)| {
                    head_option.map(|head| HeadTail { head, tail })
                }),
            );
        }

        if let Some(ref mut next_stream) = this.next {
            if let Some(item) = ready!(next_stream.next().poll_unpin(cx)) {
                this.heap.push(HeadTail {
                    head: item,
                    tail: this.next.take().unwrap(),
                });
            }
        }

        match this.heap.pop() {
            None => Poll::Ready(None),
            Some(HeadTail { head, tail }) => {
                this.next.replace(tail);

                Poll::Ready(Some(head))
            }
        }
    }
}

// Just a helper function to ensure the streams we're returning all have the
// right implementations.
fn assert_stream<T, S>(stream: S) -> S
where
    S: Stream<Item = T>,
{
    stream
}

#[cfg(test)]
mod test {
    use std::pin::Pin;
    use std::time::Duration;

    use futures::stream;
    use futures::FutureExt;
    use futures::Stream;
    use futures::StreamExt;
    use tokio::sync::oneshot;
    use tokio::time;
    use tokio_stream::wrappers::IntervalStream;

    use super::*;

    #[tokio::test]
    async fn sync() {
        let streams = vec![stream::iter(vec![5, 3, 1]), stream::iter(vec![4, 3, 2])];

        assert_eq!(
            kmerge(streams).collect::<Vec<usize>>().await,
            vec![5, 4, 3, 3, 2, 1],
        );
    }

    #[tokio::test]
    async fn by() {
        let streams = vec![stream::iter(vec![5, 3, 1]), stream::iter(vec![4, 3, 2])];
        let stream = kmerge_by(streams, |x: &usize, y: &usize| x.cmp(&y));

        assert_eq!(stream.collect::<Vec<usize>>().await, vec![5, 4, 3, 3, 2, 1],);
    }

    #[tokio::test]
    async fn by_key() {
        let streams = vec![
            stream::iter(vec![("a", 5), ("a", 3)]),
            stream::iter(vec![("b", 4), ("b", 4)]),
        ];
        let stream = kmerge_by_key(streams, |x: &(&'static str, usize)| x.1);

        assert_eq!(
            stream.collect::<Vec<_>>().await,
            vec![("a", 5), ("b", 4), ("b", 4), ("a", 3)]
        );
    }

    #[tokio::test]
    async fn kmerge_async() {
        let streams = vec![
            IntervalStream::new(time::interval(Duration::from_nanos(1))),
            IntervalStream::new(time::interval(Duration::from_nanos(2))),
        ];

        let result = kmerge(streams).take(10).collect::<Vec<_>>().await;

        assert_eq!(result.len(), 10);
    }

    #[tokio::test]
    async fn concurrent_initialization() {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let s1 = async move {
            tx1.send(1).unwrap();
            rx2.await.unwrap()
        }
        .into_stream();
        let s2 = async move {
            tx2.send(2).unwrap();
            rx1.await.unwrap()
        }
        .into_stream();

        let streams: Vec<Pin<Box<dyn Stream<Item = i32>>>> = vec![Box::pin(s1), Box::pin(s2)];

        let result = kmerge(streams).collect::<Vec<_>>().await;
        assert_eq!(result, vec![2, 1]);
    }
}

#[cfg(doctest)]
doc_comment::doctest!("../README.md");
