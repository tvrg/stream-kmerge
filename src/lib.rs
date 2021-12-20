use std::cmp::Ordering;
use std::task::Poll;

use futures::{Stream, StreamExt};
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
    {
        #[pin]
        unpolled: Vec<S>,
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
        let mut this = self.project();
        while let Some(mut s) = this.unpolled.pop() {
            let next = s.poll_next_unpin(cx);
            match next {
                Poll::Ready(Some(item)) => {
                    let pos = this
                        .heap
                        .binary_search_by(|x| (this.compare)(&item, &x.head))
                        .unwrap_or_else(|pos| pos);
                    this.heap.insert(
                        pos,
                        HeadTail {
                            head: item,
                            tail: s,
                        },
                    )
                }
                Poll::Ready(None) => (),
                Poll::Pending => {
                    this.unpolled.push(s);
                    return Poll::Pending;
                }
            };
        }
        if !this.unpolled.is_empty() {
            return Poll::Pending;
        }

        match this.heap.pop() {
            Some(HeadTail { head, tail }) => {
                this.unpolled.push(tail);
                Poll::Ready(Some(head))
            }
            None => Poll::Ready(None),
        }
    }
}

pub fn merge_by<S, F>(xs: Vec<S>, compare: F) -> KWayMerge<S, F>
where
    S: Stream,
    F: FnMut(&S::Item, &S::Item) -> Ordering,
{
    KWayMerge {
        unpolled: xs,
        heap: Vec::new(),
        compare,
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use futures::stream;
    use futures::StreamExt;
    use tokio::time;
    use tokio_stream::wrappers::IntervalStream;

    use super::merge_by;

    #[tokio::test]
    async fn test() {
        let streams = vec![stream::iter(vec![1, 3, 5]), stream::iter(vec![2, 3, 4])];

        assert_eq!(
            merge_by(streams, |x, y| x.cmp(y))
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

        let result = merge_by(streams, |x, y| x.cmp(y))
            .take(10)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 10);
    }
}
