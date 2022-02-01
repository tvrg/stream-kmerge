use std::task::Poll;

use futures::future::{join_all, JoinAll};
use futures::{ready, stream::StreamFuture, FutureExt, Stream, StreamExt};
use pin_project_lite::pin_project;

#[derive(Debug)]
struct HeadTail<S>
where
    S: Stream,
{
    head: S::Item,
    tail: S,
}

/// Make `data` a heap (min-heap w.r.t the sorting).
fn heapify<T, S>(data: &mut [T], mut less_than: S)
where
    S: FnMut(&T, &T) -> bool,
{
    for i in (0..data.len() / 2).rev() {
        sift_down(data, i, &mut less_than);
    }
}

/// Sift down element at `index` (`heap` is a min-heap wrt the ordering)
fn sift_down<T, S>(heap: &mut [T], index: usize, mut less_than: S)
where
    S: FnMut(&T, &T) -> bool,
{
    debug_assert!(index <= heap.len());
    let mut pos = index;
    let mut child = 2 * pos + 1;
    // Require the right child to be present
    // This allows to find the index of the smallest child without a branch
    // that wouldn't be predicted if present
    while child + 1 < heap.len() {
        // pick the smaller of the two children
        // use aritmethic to avoid an unpredictable branch
        child += less_than(&heap[child + 1], &heap[child]) as usize;

        // sift down is done if we are already in order
        if !less_than(&heap[child], &heap[pos]) {
            return;
        }
        heap.swap(pos, child);
        pos = child;
        child = 2 * pos + 1;
    }
    // Check if the last (left) child was an only child
    // if it is then it has to be compared with the parent
    if child + 1 == heap.len() && less_than(&heap[child], &heap[pos]) {
        heap.swap(pos, child);
    }
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
    pub struct KWayMergeBy<S, F>
    where
        S: Stream,
        S: Unpin,
    {
        initial: Option<JoinAll<StreamFuture<S>>>,
        next: Option<S>,
        heap: Vec<HeadTail<S>>,
        less_than: F,
    }
}

/// A stream adaptor that merges an abitrary number of base streams in ascending order.
/// If all base streams are sorted (ascending), the result is sorted.
///
/// Stream element type is `S::Item`.
///
/// See [`.kmerge()`](crate::kmerge) for more information.
#[must_use = "stream adaptors are lazy and do nothing unless consumed"]
pub type KWayMerge<I> = KWayMergeBy<I, KMergeByLt>;

pub trait KMergePredicate<T> {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> bool;
}

#[derive(Clone, Debug)]
pub struct KMergeByLt;

impl<T: PartialOrd> KMergePredicate<T> for KMergeByLt {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> bool {
        a < b
    }
}

impl<T, F: FnMut(&T, &T) -> bool> KMergePredicate<T> for F {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> bool {
        self(a, b)
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
/// let streams = vec![stream::iter(vec![1, 3, 5]), stream::iter(vec![2, 3, 4])];
///
/// assert_eq!(
///     kmerge(streams).collect::<Vec<usize>>().await,
///     vec![1, 2, 3, 3, 4, 5],
/// );
/// # })
/// ```
pub fn kmerge<S>(xs: impl IntoIterator<Item = S>) -> KWayMerge<S>
where
    S: Stream + Unpin,
    S::Item: PartialOrd,
{
    assert_stream::<S::Item, _>(kmerge_generic(xs, KMergeByLt))
}

/// Create a stream that merges elements of the contained streams.
///
/// ```
/// # tokio_test::block_on(async {
/// use futures::{stream, StreamExt};
/// use stream_kmerge::kmerge_by;
///
/// let streams = vec![stream::iter(vec![5, 3, 1]), stream::iter(vec![4, 3, 2])];
///
/// assert_eq!(
///     kmerge_by(streams, |x, y| y < x).collect::<Vec<usize>>().await,
///     vec![5, 4, 3, 3, 2, 1],
/// );
/// # })
/// ```
pub fn kmerge_by<S, F>(
    xs: impl IntoIterator<Item = S>,
    less_than: F,
) -> KWayMergeBy<S, F>
where
    S: Stream + Unpin,
    F: FnMut(&S::Item, &S::Item) -> bool,
{
    kmerge_generic(xs, less_than)
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
fn kmerge_generic<S, F>(
    xs: impl IntoIterator<Item = S>,
    less_than: F,
) -> KWayMergeBy<S, F>
where
    S: Stream + Unpin,
    F: KMergePredicate<S::Item>,
{
    let iter = xs.into_iter();
    let (min_size, _) = iter.size_hint();
    assert_stream::<S::Item, _>(KWayMergeBy {
        initial: Some(join_all(iter.map(|x| x.into_future()))),
        next: None,
        heap: Vec::with_capacity(min_size),
        less_than,
    })
}

impl<S, F> Stream for KWayMergeBy<S, F>
where
    S: Stream + Unpin,
    F: KMergePredicate<S::Item>,
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
            heapify(this.heap, |a, b| {
                this.less_than.kmerge_pred(&a.head, &b.head)
            });
        }

        if let Some(ref mut next_stream) = this.next {
            if let Some(item) = ready!(next_stream.next().poll_unpin(cx)) {
                this.heap.insert(
                    0,
                    HeadTail {
                        head: item,
                        tail: this.next.take().unwrap(),
                    },
                );
                sift_down(this.heap, 0, |a, b| {
                    this.less_than.kmerge_pred(&a.head, &b.head)
                });
            }
        }

        if this.heap.is_empty() {
            return Poll::Ready(None);
        }

        let HeadTail { head, tail } = this.heap.remove(0);
        this.next.replace(tail);
        Poll::Ready(Some(head))
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
        let streams = vec![stream::iter(vec![1, 3, 5]), stream::iter(vec![2, 3, 4])];

        assert_eq!(
            kmerge(streams).collect::<Vec<usize>>().await,
            vec![1, 2, 3, 3, 4, 5],
        );
    }

    #[tokio::test]
    async fn by() {
        let streams = vec![stream::iter(vec![5, 3, 1]), stream::iter(vec![4, 3, 2])];
        let stream = kmerge_by(streams, |x, y| y < x);

        assert_eq!(stream.collect::<Vec<usize>>().await, vec![5, 4, 3, 3, 2, 1],);
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
        assert_eq!(result, vec![1, 2]);
    }
}
