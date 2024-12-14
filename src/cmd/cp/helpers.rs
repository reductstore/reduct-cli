// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;
use std::time::Duration;

use bytesize::ByteSize;
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reduct_rs::{Bucket, EntryInfo, ErrorCode, QueryBuilder, Record, ReductError};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};

use crate::parse::{fetch_and_filter_entries, QueryParams};

pub(super) struct TransferProgress {
    entry: EntryInfo,
    transferred_bytes: u64,
    record_count: u64,
    speed: u64,
    history: Vec<(u64, Instant)>,
    speed_update: Instant,
    progress_bar: ProgressBar,
    start: Option<u64>,
}

impl TransferProgress {
    pub(super) fn new(
        entry: EntryInfo,
        query_params: &QueryParams,
        progress: &MultiProgress,
    ) -> Self {
        let (start, progress_bar) = init_task_progress_bar(query_params, progress, &entry);

        let me = Self {
            entry,
            transferred_bytes: 0,
            record_count: 0,
            speed: 0,
            history: Vec::new(),
            speed_update: Instant::now(),
            progress_bar,
            start,
        };

        me.progress_bar.set_message(me.message());
        me
    }

    pub(super) fn update(&mut self, time: u64, content_length: u64) {
        self.transferred_bytes += content_length;
        self.record_count += 1;
        self.history.push((content_length, Instant::now()));

        let duration = self.history[0].1.elapsed().as_secs();
        self.speed =
            if self.speed_update.elapsed().as_secs() > 3 && self.history.len() > 10 && duration > 0
            {
                let result = self.history.iter().map(|(bytes, _)| bytes).sum::<u64>() / duration;
                self.history.clear();
                self.speed_update = Instant::now();
                result
            } else {
                self.speed
            };

        if let Some(start) = self.start {
            self.progress_bar.set_position(time - start);
        } else {
            //  query with limit
            self.progress_bar.set_position(self.record_count);
        }

        self.progress_bar.set_message(self.message());
    }

    pub(crate) fn print_error(&self, err: String) {
        self.progress_bar.set_message(format!("{}", err));
        self.progress_bar.abandon();
    }

    pub(crate) fn done(&self) {
        let msg = format!(
            "Copied {} records from '{}' ({}, {}/s)",
            self.record_count,
            self.entry.name,
            ByteSize::b(self.transferred_bytes),
            ByteSize::b(self.speed)
        );

        self.progress_bar.set_message(msg);
        self.progress_bar.abandon();
    }

    fn message(&self) -> String {
        format!(
            "Copying {} records from '{}' ({}, {}/s)",
            self.record_count,
            self.entry.name,
            ByteSize::b(self.transferred_bytes),
            ByteSize::b(self.speed)
        )
    }
}

fn build_query(src_bucket: &Bucket, entry: &EntryInfo, query_params: &QueryParams) -> QueryBuilder {
    let mut query_builder = src_bucket.query(&entry.name);
    if let Some(start) = query_params.start {
        query_builder = query_builder.start_us(start);
    }

    if let Some(stop) = query_params.stop {
        query_builder = query_builder.stop_us(stop);
    }

    if let Some(each_n) = query_params.each_n {
        query_builder = query_builder.each_n(each_n);
    }

    if let Some(each_s) = query_params.each_s {
        query_builder = query_builder.each_s(each_s);
    }

    if let Some(when) = &query_params.when {
        query_builder = query_builder.when(when.clone());
    }

    query_builder = query_builder.include(query_params.include_labels.clone());
    query_builder = query_builder.exclude(query_params.exclude_labels.clone());
    query_builder = query_builder.strict(query_params.strict);

    if let Some(limit) = query_params.limit {
        query_builder = query_builder.limit(limit);
    }

    query_builder.ttl(query_params.ttl)
}

#[async_trait::async_trait]
pub(super) trait CopyVisitor {
    async fn visit(&self, entry_name: &str, record: Record) -> Result<(), ReductError>;
}

/**
 * Start loading records from the source bucket and move to a visitor
 *
 * # Arguments
 *
 * `src_bucket` - The source bucket
 * `query_params` - The query parameters. Use `parse_query_params` to parse the arguments
 * `visitor` - The visitor that will receive the records
 */
pub(super) async fn start_loading<V>(
    src_bucket: Bucket,
    query_params: QueryParams,
    visitor: V,
) -> anyhow::Result<()>
where
    V: CopyVisitor + Send + Sync + 'static,
{
    let entries = fetch_and_filter_entries(&src_bucket, &query_params.entry_filter).await?;

    let mut tasks = JoinSet::new();
    let progress = MultiProgress::new();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(query_params.parallel));

    let visitor = Arc::new(visitor);
    for entry in entries {
        let query_builder = build_query(&src_bucket, &entry, &query_params);
        let local_sem = Arc::clone(&semaphore);
        let mut transfer_progress = TransferProgress::new(entry.clone(), &query_params, &progress);

        let local_visitor = Arc::clone(&visitor);
        tasks.spawn(async move {
            macro_rules! print_error_progress {
                ($err:expr) => {{
                    transfer_progress.print_error($err.to_string());
                    Err($err)
                }};
            }
            let _permit = local_sem.acquire().await.unwrap();

            let record_stream = match query_builder.send().await {
                Ok(stream) => stream,
                Err(err) => {
                    return print_error_progress!(err);
                }
            };

            tokio::pin!(record_stream);

            while let Some(record) = record_stream.next().await {
                if let Err(err) = record {
                    return print_error_progress!(err);
                }

                let record = record?;
                let timestamp = record.timestamp_us();

                let content_length = record.content_length() as u64;
                let result = local_visitor.visit(&entry.name, record).await;

                if let Err(err) = result {
                    // ignore conflict errors
                    if err.status() != ErrorCode::Conflict {
                        return print_error_progress!(err);
                    }
                }

                transfer_progress.update(timestamp, content_length);
                sleep(Duration::from_micros(5)).await;
            }

            transfer_progress.done();

            Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        let _ = result?;
    }

    Ok(())
}

fn init_task_progress_bar(
    query_params: &QueryParams,
    progress: &MultiProgress,
    entry: &EntryInfo,
) -> (Option<u64>, ProgressBar) {
    let limit = query_params.limit;
    let sty = ProgressStyle::with_template(
        "[{elapsed_precise}, ETA {eta_precise}] {bar:40.green/gray} {percent_precise:6}% {msg}",
    )
    .unwrap();

    if let Some(limit) = limit {
        // progress for limited copying
        let local_progress = ProgressBar::new(limit);
        local_progress.set_style(sty.clone());

        (None, progress.add(local_progress))
    } else {
        // progress for full copying based on time range
        // we don't know the total number of records, so we use the time range
        let start = if let Some(start) = query_params.start {
            start as u64
        } else {
            entry.oldest_record
        };

        let stop = if let Some(stop) = query_params.stop {
            stop as u64
        } else {
            entry.latest_record
        };

        let local_progress = ProgressBar::new(stop - start);
        let local_progress = progress.add(local_progress);
        local_progress.set_style(sty.clone());

        (Some(start), local_progress)
    }
}

#[cfg(test)]
mod tests {
    use rstest::*;

    use super::*;

    mod downloading {
        use crate::context::tests::{bucket, context};
        use crate::context::CliContext;
        use crate::io::reduct::build_client;
        use async_trait::async_trait;
        use bytes::Bytes;
        use mockall::mock;
        use mockall::predicate::{always, eq};
        use reduct_rs::Labels;

        use super::*;

        mock! {
            pub Visitor {}
            #[async_trait]
            impl CopyVisitor for Visitor {
                async fn visit(&self, entry_name: &str, record: Record) -> Result<(), ReductError>;
            }
        }
        #[fixture]
        fn visitor() -> MockVisitor {
            MockVisitor::new()
        }

        #[fixture]
        async fn src_bucket(context: CliContext, #[future] bucket: String) -> Bucket {
            let client = build_client(&context, "local").await.unwrap();
            let bucket = client.create_bucket(&bucket.await).send().await.unwrap();
            bucket
                .write_record("entry-1")
                .data(Bytes::from_static(b"rec-1"))
                .send()
                .await
                .unwrap();
            bucket
                .write_record("entry-2")
                .data(Bytes::from_static(b"rec-2"))
                .send()
                .await
                .unwrap();

            bucket
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading(#[future] src_bucket: Bucket, mut visitor: MockVisitor) {
            let src_bucket = src_bucket.await;
            visitor.expect_visit().times(2).return_const(Ok(()));

            start_loading(src_bucket, QueryParams::default(), visitor)
                .await
                .unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_metadata(#[future] src_bucket: Bucket, mut visitor: MockVisitor) {
            let src_bucket = src_bucket.await;
            visitor
                .expect_visit()
                .withf(|entry, _record| entry == "entry-1")
                .times(1)
                .return_const(Ok(()));
            visitor
                .expect_visit()
                .withf(|entry, _record| entry == "entry-2")
                .times(1)
                .return_const(Ok(()));
            visitor
                .expect_visit()
                .withf(|entry, record| {
                    entry == "entry-3"
                        && record.timestamp_us() == 1
                        && record.labels().get("key").unwrap() == "value"
                        && record.content_type() == "text/plain"
                })
                .times(1)
                .return_const(Ok(()));

            src_bucket
                .write_record("entry-3")
                .timestamp_us(1)
                .add_label("key", "value")
                .content_type("text/plain")
                .data(Bytes::from_static(b"rec-3"))
                .send()
                .await
                .unwrap();

            start_loading(src_bucket, QueryParams::default(), visitor)
                .await
                .unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_finish_rest_if_one_fails(
            #[future] src_bucket: Bucket,
            mut visitor: MockVisitor,
        ) {
            let src_bucket = src_bucket.await;
            visitor
                .expect_visit()
                .withf(|entry, _record| entry == "entry-1")
                .times(1)
                .return_const(Err(ReductError::new(ErrorCode::Conflict, "Conflict")));
            visitor
                .expect_visit()
                .withf(|entry, _record| entry == "entry-2")
                .times(1)
                .return_const(Ok(()));

            let result = start_loading(src_bucket, QueryParams::default(), visitor).await;
            assert!(result.is_ok());
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_entry_wildcard(
            #[future] src_bucket: Bucket,
            mut visitor: MockVisitor,
        ) {
            let src_bucket = src_bucket.await;
            visitor.expect_visit().times(2).return_const(Ok(()));

            let params = QueryParams {
                entry_filter: vec!["entry-*".to_string()],
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_entry_filter(
            #[future] src_bucket: Bucket,
            mut visitor: MockVisitor,
        ) {
            let src_bucket = src_bucket.await;
            visitor
                .expect_visit()
                .times(1)
                .with(eq("entry-1"), always())
                .return_const(Ok(()));

            let params = QueryParams {
                entry_filter: vec!["entry-1".to_string()],
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_limit(#[future] src_bucket: Bucket, mut visitor: MockVisitor) {
            let src_bucket = src_bucket.await;
            visitor.expect_visit().times(2).return_const(Ok(()));

            src_bucket
                .write_record("entry-1")
                .data(Bytes::from_static(b"rec-3"))
                .send()
                .await
                .unwrap();
            src_bucket
                .write_record("entry-2")
                .data(Bytes::from_static(b"rec-4"))
                .send()
                .await
                .unwrap();

            let params = QueryParams {
                limit: Some(1),
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_include(#[future] src_bucket: Bucket, mut visitor: MockVisitor) {
            let src_bucket = src_bucket.await;
            visitor
                .expect_visit()
                .times(1)
                .with(eq("entry-1"), always())
                .return_const(Ok(()));

            src_bucket
                .write_record("entry-1")
                .data(Bytes::from_static(b"rec-3"))
                .add_label("key1", "value1")
                .send()
                .await
                .unwrap();

            let params = QueryParams {
                include_labels: Labels::from_iter(vec![("key1".to_string(), "value1".to_string())]),
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_exclude(#[future] src_bucket: Bucket, mut visitor: MockVisitor) {
            let src_bucket = src_bucket.await;
            visitor
                .expect_visit()
                .times(1)
                .with(eq("entry-1"), always())
                .return_const(Ok(()));
            visitor
                .expect_visit()
                .times(1)
                .with(eq("entry-2"), always())
                .return_const(Ok(()));

            src_bucket
                .write_record("entry-1")
                .data(Bytes::from_static(b"rec-3"))
                .add_label("key1", "value1")
                .send()
                .await
                .unwrap();

            let params = QueryParams {
                exclude_labels: Labels::from_iter(vec![("key1".to_string(), "value1".to_string())]),
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }
    }
}
