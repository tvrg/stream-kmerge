use std::cmp::Ordering;
use std::task::Poll;

use futures::{
    future::{join_all, JoinAll},
    ready,
    stream::StreamFuture,
    FutureExt, Stream, StreamExt,
};
use pin_project_lite::pin_project;

struct HeadTail<S>
where
    S: Stream,
{
    head: S::Item,
    tail: S,
}

impl<S> PartialEq for HeadTail<S>
where
    S: Stream,
    S::Item: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.head.eq(&other.head)
    }
}

impl<S> Eq for HeadTail<S>
where
    S: Stream,
    S::Item: Eq,
{
}

pub trait KMergePredicate<T> {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> Ordering;
}

#[derive(Clone, Debug)]
pub struct KMergeByLt;

impl<T: Ord> KMergePredicate<T> for KMergeByLt {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> Ordering {
        a.cmp(b)
    }
}

impl<T, F: FnMut(&T, &T) -> Ordering> KMergePredicate<T> for F {
    fn kmerge_pred(&mut self, a: &T, b: &T) -> Ordering {
        self(a, b)
    }
}

pin_project! {
    pub struct KWayMerge<S, F>
    where
        S: Stream,
        S: Unpin,
    {
        initial: Option<JoinAll<StreamFuture<S>>>,
        next: Option<S>,
        heap: Vec<HeadTail<S>>,
        compare: F,
    }
}

impl<S, F> Stream for KWayMerge<S, F>
where
    S: Stream,
    S: Unpin,
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
            xs.into_iter()
                .filter_map(|(head_option, tail)| head_option.map(|head| HeadTail { head, tail }))
                .for_each(|head_tail| this.heap.push(head_tail));
            this.heap
                .sort_unstable_by(|x, y| this.compare.kmerge_pred(&y.head, &x.head));
        }

        if let Some(ref mut next_stream) = this.next {
            if let Some(item) = ready!(next_stream.next().poll_unpin(cx)) {
                let pos = this
                    .heap
                    .binary_search_by(|x| this.compare.kmerge_pred(&item, &x.head))
                    .unwrap_or_else(|pos| pos);
                this.heap.insert(
                    pos,
                    HeadTail {
                        head: item,
                        tail: this.next.take().unwrap(),
                    },
                )
            }
        }

        match this.heap.pop() {
            Some(HeadTail { head, tail }) => {
                this.next.replace(tail);
                Poll::Ready(Some(head))
            }
            None => Poll::Ready(None),
        }
    }
}

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
pub fn kmerge<S>(xs: impl IntoIterator<Item = S>) -> KWayMerge<S, KMergeByLt>
where
    S: Stream + Unpin,
    S::Item: Ord,
{
    kmerge_by(xs, KMergeByLt)
}

pub fn kmerge_by<S, F>(xs: impl IntoIterator<Item = S>, compare: F) -> KWayMerge<S, F>
where
    S: Stream + Unpin,
    F: KMergePredicate<S::Item>,
{
    KWayMerge {
        initial: Some(join_all(xs.into_iter().map(|x| x.into_future()))),
        next: None,
        heap: Vec::new(),
        compare,
    }
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

    use super::kmerge;

    #[tokio::test]
    async fn test() {
        let streams = vec![stream::iter(vec![1, 3, 5]), stream::iter(vec![2, 3, 4])];

        assert_eq!(
            kmerge(streams).collect::<Vec<usize>>().await,
            vec![1, 2, 3, 3, 4, 5],
        );
    }

    #[tokio::test]
    async fn test_async() {
        let streams = vec![
            IntervalStream::new(time::interval(Duration::from_nanos(1))),
            IntervalStream::new(time::interval(Duration::from_nanos(2))),
        ];

        let result = kmerge(streams).take(10).collect::<Vec<_>>().await;

        assert_eq!(result.len(), 10);
    }

    #[tokio::test]
    async fn test_concurrent_initialization() {
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
