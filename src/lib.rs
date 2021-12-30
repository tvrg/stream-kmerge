use std::cmp::Ordering;
use std::task::Poll;

use futures::{
    future::{join_all, JoinAll},
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

impl<S> PartialOrd for HeadTail<S>
where
    S: Stream,
    S::Item: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.head
            .partial_cmp(&other.head)
            .map(std::cmp::Ordering::reverse)
    }
}

impl<S> Ord for HeadTail<S>
where
    S: Stream,
    S::Item: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.head.cmp(&other.head).reverse()
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
    F: FnMut(&S::Item, &S::Item) -> Ordering,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        if let Some(init_fut) = this.initial.as_mut() {
            match init_fut.poll_unpin(cx) {
                Poll::Pending => {
                    return Poll::Pending;
                }
                Poll::Ready(xs) => {
                    *this.initial = None;
                    xs.into_iter()
                        .filter_map(|(head_option, tail)| {
                            head_option.map(|head| HeadTail { head, tail })
                        })
                        .for_each(|head_tail| this.heap.push(head_tail));
                    this.heap
                        .sort_unstable_by(|x, y| (this.compare)(&y.head, &x.head));
                }
            }
        }
        if let Some(mut next_stream) = this.next.take() {
            match next_stream.next().poll_unpin(cx) {
                Poll::Pending => {
                    this.next.replace(next_stream);
                    return Poll::Pending;
                }
                Poll::Ready(Some(item)) => {
                    let pos = this
                        .heap
                        .binary_search_by(|x| (this.compare)(&item, &x.head))
                        .unwrap_or_else(|pos| pos);
                    this.heap.insert(
                        pos,
                        HeadTail {
                            head: item,
                            tail: next_stream,
                        },
                    )
                }
                Poll::Ready(None) => {}
            }
        }

        if this.next.is_some() {
            return Poll::Pending;
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

pub fn kmerge_by<S, F>(xs: impl IntoIterator<Item = S>, compare: F) -> KWayMerge<S, F>
where
    S: Stream + Unpin,
    F: Fn(&S::Item, &S::Item) -> Ordering,
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

    use super::kmerge_by;

    #[tokio::test]
    async fn test() {
        let streams = vec![stream::iter(vec![1, 3, 5]), stream::iter(vec![2, 3, 4])];

        assert_eq!(
            kmerge_by(streams, |x, y| x.cmp(y))
                .collect::<Vec<usize>>()
                .await,
            vec![1, 2, 3, 3, 4, 5],
        );
    }

    #[tokio::test]
    async fn test_async() {
        let streams = vec![
            IntervalStream::new(time::interval(Duration::from_nanos(1))),
            IntervalStream::new(time::interval(Duration::from_nanos(2))),
        ];

        let result = kmerge_by(streams, |x, y| x.cmp(y))
            .take(10)
            .collect::<Vec<_>>()
            .await;

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

        let result = kmerge_by(streams, |x, y| x.cmp(y)).collect::<Vec<_>>().await;
        assert_eq!(result, vec![1, 2]);
    }
}
