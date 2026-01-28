// Copyright 2024-2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::parse::{fetch_and_filter_entries, QueryParams};
use bytesize::ByteSize;
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reduct_rs::{condition, Bucket, EntryInfo, QueryBuilder, Record, ReductError};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};

const DOWNLOAD_ATTEMPTS: u8 = 10;
const NUMBER_DOWNLOAD_LIMIT: i8 = 80;
const MEMORY_AMOUNT_LIMIT: isize = 8192; //8000?  1000 kB kilobyte, 1024 KiB kibibyte
pub(super) struct BucketProgress {
    bucket_name: String,
    displayed_entries: Vec<String>,
    display_limit: usize,
    total_entries: usize,
    entry_stats: HashMap<String, (u64, u64)>,
    transferred_bytes: u64,
    record_count: u64,
    speed: u64,
    history: Vec<(u64, Instant)>,
    speed_update: Instant,
    progress_bar: ProgressBar,
    quiet: bool,
}

impl BucketProgress {
    pub(super) fn new(
        bucket_name: String,
        display_limit: usize,
        total_entries: usize,
        progress: &MultiProgress,
        quiet: bool,
    ) -> Self {
        let progress_bar = init_bucket_progress_bar(progress, quiet);

        let me = Self {
            bucket_name,
            displayed_entries: Vec::new(),
            display_limit: display_limit.max(1),
            total_entries,
            entry_stats: HashMap::new(),
            transferred_bytes: 0,
            record_count: 0,
            speed: 0,
            history: Vec::new(),
            speed_update: Instant::now(),
            progress_bar,
            quiet,
        };

        if !me.quiet {
            me.progress_bar.set_message(me.message());
        }
        me
    }

    pub(super) fn update(&mut self, entry_name: &str, content_length: u64) {
        self.transferred_bytes += content_length;
        self.record_count += 1;
        self.history.push((content_length, Instant::now()));
        let entry_stats = self
            .entry_stats
            .entry(entry_name.to_string())
            .or_insert((0, 0));
        let was_empty = entry_stats.0 == 0;
        entry_stats.0 = entry_stats.0.saturating_add(1);
        entry_stats.1 = entry_stats.1.saturating_add(content_length);
        if was_empty
            && self.displayed_entries.len() < self.display_limit
            && !self.displayed_entries.iter().any(|name| name == entry_name)
        {
            self.displayed_entries.push(entry_name.to_string());
        }

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

        if !self.quiet {
            self.progress_bar
                .set_length(self.record_count.saturating_add(1));
            self.progress_bar.set_position(self.record_count);
            self.progress_bar.set_message(self.message());
        }
    }

    pub(crate) fn print_warning(&self, warning: String) {
        if !self.quiet {
            self.progress_bar
                .set_message(format!("Warning: {} | {}", warning, self.message()));
        }
    }

    pub(crate) fn done(&self) {
        if !self.quiet {
            let msg = format!(
                "Copied {} records from bucket '{}' ({}, {}/s)\n{}",
                self.record_count,
                self.bucket_name,
                ByteSize::b(self.transferred_bytes),
                ByteSize::b(self.speed),
                self.entry_tree()
            );
            self.progress_bar.set_message(msg);
            self.progress_bar.abandon();
        } else {
            let msg = format!(
                "Copied {} records from bucket '{}' ({})",
                self.record_count,
                self.bucket_name,
                ByteSize::b(self.transferred_bytes)
            );
            println!("{}", msg);
        }
    }

    pub(crate) fn all_failed(&self) {
        if !self.quiet {
            let msg = format!(
                "Failed to copy any entries from bucket '{}'\n{}",
                self.bucket_name,
                self.entry_tree()
            );
            self.progress_bar.set_message(msg);
            self.progress_bar.abandon();
        } else {
            let msg = format!(
                "Failed to copy any entries from bucket '{}'",
                self.bucket_name
            );
            eprintln!("{}", msg);
        }
    }

    fn message(&self) -> String {
        format!(
            "Copying {} records from bucket '{}' ({}, {}/s)\n{}",
            self.record_count,
            self.bucket_name,
            ByteSize::b(self.transferred_bytes),
            ByteSize::b(self.speed),
            self.entry_tree()
        )
    }

    fn entry_tree(&self) -> String {
        let mut lines = Vec::with_capacity(self.displayed_entries.len() + 2);
        lines.push("entries:".to_string());
        if self.displayed_entries.is_empty() {
            lines.push("  └─ (no entries copied yet)".to_string());
            return lines.join("\n");
        }
        for (idx, entry) in self.displayed_entries.iter().enumerate() {
            let prefix = if idx + 1 == self.displayed_entries.len()
                && self.total_entries <= self.displayed_entries.len()
            {
                "└─ "
            } else if idx + 1 == self.displayed_entries.len() {
                "└─ "
            } else {
                "├─ "
            };
            let (records, bytes) = self.entry_stats.get(entry).copied().unwrap_or((0, 0));
            lines.push(format!(
                "  {}{} (🧷 {}, 📦 {})",
                prefix,
                entry,
                records,
                ByteSize::b(bytes)
            ));
        }
        if self.total_entries > self.displayed_entries.len() {
            lines.push(format!(
                "  └─ +{} more",
                self.total_entries - self.displayed_entries.len()
            ));
        }
        lines.join("\n")
    }
}

fn build_query(src_bucket: &Bucket, entry: &EntryInfo, query_params: &QueryParams) -> QueryBuilder {
    let mut query_builder = src_bucket.query(entry.name.as_str());
    if let Some(start) = query_params.start {
        query_builder = query_builder.start_us(start);
    }

    if let Some(stop) = query_params.stop {
        query_builder = query_builder.stop_us(stop);
    }

    let mut when: serde_json::Value = query_params.when.clone().unwrap_or(condition!({}));
    if let Some(obj) = when.as_object_mut() {
        if let Some(each_n) = query_params.each_n {
            obj.insert("$each_n".to_string(), serde_json::json!(each_n));
        }
        if let Some(each_s) = query_params.each_s {
            obj.insert("$each_s".to_string(), serde_json::json!(each_s));
        }
        if let Some(limit) = query_params.limit {
            obj.insert("$limit".to_string(), serde_json::json!(limit));
        }
    }

    if let Some(ext) = &query_params.ext {
        query_builder = query_builder.ext(ext.clone());
    }

    query_builder = query_builder.strict(query_params.strict);

    query_builder = query_builder.when(when);

    query_builder.ttl(query_params.ttl)
}

#[async_trait::async_trait]
pub(super) trait CopyVisitor {
    async fn visit(
        &self,
        entry_name: &str,
        records: Vec<Record>,
    ) -> Result<BTreeMap<u64, ReductError>, ReductError>;
}

/**
 * Start loading records from the source bucket and move to a visitor
 *
 * # Arguments
 *
 * `src_bucket` - The source bucket
 * `query_params` - The query parameters. Use `parse_query_params` to parse the arguments
 * `dst_bucket_v` - The dst_bucket_v that will receive the records
 */
pub(super) async fn start_loading<V>(
    src_bucket: Bucket,
    query_params: QueryParams,
    dst_bucket_v: V,
) -> anyhow::Result<()>
where
    V: CopyVisitor + Send + Sync + 'static,
{
    start_loading_with_entry_start_overrides(src_bucket, query_params, None, dst_bucket_v).await
}

pub(super) async fn start_loading_with_entry_start_overrides<V>(
    src_bucket: Bucket,
    query_params: QueryParams,
    entry_start_overrides: Option<HashMap<String, u64>>,
    dst_bucket_v: V,
) -> anyhow::Result<()>
where
    V: CopyVisitor + Send + Sync + 'static,
{
    let entries = fetch_and_filter_entries(&src_bucket, &query_params.entry_filter).await?;
    if entries.is_empty() {
        return Ok(());
    }

    let mut tasks = JoinSet::new();
    let progress = MultiProgress::new();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(query_params.parallel));

    let dst_bucket = Arc::new(dst_bucket_v);
    let src_bucket = Arc::new(src_bucket);
    let entry_start_overrides = entry_start_overrides.map(Arc::new);

    let display_limit = query_params.parallel.max(1);
    let bucket_name = src_bucket.name().to_string();
    let quiet = query_params.quiet;
    let bucket_progress = Arc::new(Mutex::new(BucketProgress::new(
        bucket_name,
        display_limit,
        entries.len(),
        &progress,
        quiet,
    )));

    for entry in entries {
        let local_sem = Arc::clone(&semaphore);
        let local_visitor = Arc::clone(&dst_bucket);
        let params = build_entry_params(&query_params, &entry, entry_start_overrides.as_deref());
        let bucket_progress = Arc::clone(&bucket_progress);
        let bucket = Arc::clone(&src_bucket);
        tasks.spawn(run_entry_copy(
            entry,
            bucket,
            local_visitor,
            local_sem,
            params,
            bucket_progress,
        ));
    }

    let mut failed_entries = 0usize;
    let mut completed_entries = 0usize;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(outcome) => {
                completed_entries += 1;
                if let Err(err) = outcome.result {
                    failed_entries += 1;
                    eprintln!("Failed to copy entry '{}': {}", outcome.entry_name, err);
                }
            }
            Err(err) => {
                failed_entries += 1;
                completed_entries += 1;
                eprintln!("Failed to copy entry: {}", err);
            }
        }
    }

    let progress_guard = bucket_progress.lock().unwrap();
    if failed_entries == completed_entries {
        progress_guard.all_failed();
        return Err(anyhow::anyhow!(
            "Failed to copy any entries from bucket '{}'",
            progress_guard.bucket_name
        ));
    }

    progress_guard.done();
    Ok(())
}

fn build_entry_params(
    query_params: &QueryParams,
    entry: &EntryInfo,
    entry_start_overrides: Option<&HashMap<String, u64>>,
) -> QueryParams {
    let mut params = query_params.clone();
    if params.start.is_none() {
        if let Some(start) = entry_start_override(entry, entry_start_overrides) {
            params.start = Some(start);
        }
    }
    params
}

fn entry_start_override(
    entry: &EntryInfo,
    entry_start_overrides: Option<&HashMap<String, u64>>,
) -> Option<u64> {
    entry_start_overrides.and_then(|overrides| overrides.get(&entry.name).copied())
}

struct EntryCopyOutcome {
    entry_name: String,
    result: Result<(), ReductError>,
}

async fn run_entry_copy<V: CopyVisitor + ?Sized>(
    entry: EntryInfo,
    bucket: Arc<Bucket>,
    visitor: Arc<V>,
    semaphore: Arc<tokio::sync::Semaphore>,
    mut params: QueryParams,
    bucket_progress: Arc<Mutex<BucketProgress>>,
) -> EntryCopyOutcome {
    // Copy one entry with retry logic and bounded batching to control memory usage.
    let mut timestamp = params.start.unwrap_or(entry.oldest_record);
    let mut record_count = 0;
    let mut attempts = DOWNLOAD_ATTEMPTS;
    let entry_name = entry.name.clone();

    while attempts > 0 {
        let query_builder = build_query(&bucket, &entry, &params);

        let _permit = semaphore.acquire().await.unwrap();

        let record_stream = match query_builder.send().await {
            Ok(stream) => stream,
            Err(err) => {
                if let Err(e) = make_attempt(
                    &mut attempts,
                    &bucket_progress,
                    &mut params,
                    record_count,
                    timestamp,
                    err,
                )
                .await
                {
                    return EntryCopyOutcome {
                        entry_name,
                        result: Err(e),
                    };
                } else {
                    continue;
                }
            }
        };

        tokio::pin!(record_stream);

        let mut number = NUMBER_DOWNLOAD_LIMIT;
        let mut memory = MEMORY_AMOUNT_LIMIT;
        let mut batch: Vec<Record> = Vec::new();
        let mut retry = false;

        while let Some(record) = record_stream.next().await {
            let record = match record {
                Ok(record) => record,
                Err(err) => {
                    if let Err(e) = make_attempt(
                        &mut attempts,
                        &bucket_progress,
                        &mut params,
                        record_count,
                        timestamp,
                        err,
                    )
                    .await
                    {
                        return EntryCopyOutcome {
                            entry_name,
                            result: Err(e),
                        };
                    } else {
                        retry = true;
                        break;
                    }
                }
            };

            let content_length = record.content_length() as isize;
            if content_length >= MEMORY_AMOUNT_LIMIT {
                if let Err(err) = flush_batch(
                    &entry.name,
                    &mut batch,
                    visitor.as_ref(),
                    &bucket_progress,
                    &mut attempts,
                    &mut timestamp,
                    &mut record_count,
                )
                .await
                {
                    return EntryCopyOutcome {
                        entry_name,
                        result: Err(err),
                    };
                }
                batch.push(record);
                if let Err(err) = flush_batch(
                    &entry.name,
                    &mut batch,
                    visitor.as_ref(),
                    &bucket_progress,
                    &mut attempts,
                    &mut timestamp,
                    &mut record_count,
                )
                .await
                {
                    return EntryCopyOutcome {
                        entry_name,
                        result: Err(err),
                    };
                }
                number = NUMBER_DOWNLOAD_LIMIT;
                memory = MEMORY_AMOUNT_LIMIT;
                continue;
            }

            memory -= content_length;
            if memory < 0 || number == 0 {
                if let Err(err) = flush_batch(
                    &entry.name,
                    &mut batch,
                    visitor.as_ref(),
                    &bucket_progress,
                    &mut attempts,
                    &mut timestamp,
                    &mut record_count,
                )
                .await
                {
                    return EntryCopyOutcome {
                        entry_name,
                        result: Err(err),
                    };
                }
                batch.push(record);
                number = NUMBER_DOWNLOAD_LIMIT.saturating_sub(1);
                memory = MEMORY_AMOUNT_LIMIT - content_length;
            } else if number == NUMBER_DOWNLOAD_LIMIT {
                batch.push(record);
                number -= 1;
            } else {
                batch.push(record);
                number -= 1;
            }
        }

        if !retry {
            if let Err(err) = flush_batch(
                &entry.name,
                &mut batch,
                visitor.as_ref(),
                &bucket_progress,
                &mut attempts,
                &mut timestamp,
                &mut record_count,
            )
            .await
            {
                return EntryCopyOutcome {
                    entry_name,
                    result: Err(err),
                };
            }
            if attempts == DOWNLOAD_ATTEMPTS {
                break;
            }
        }
    }

    EntryCopyOutcome {
        entry_name,
        result: Ok(()),
    }
}

async fn flush_batch<V: CopyVisitor + ?Sized>(
    entry_name: &str,
    batch: &mut Vec<Record>,
    visitor: &V,
    bucket_progress: &Arc<Mutex<BucketProgress>>,
    attempts: &mut u8,
    timestamp: &mut u64,
    record_count: &mut u64,
) -> Result<(), ReductError> {
    if batch.is_empty() {
        return Ok(());
    }

    let batch_info = batch
        .iter()
        .map(|record| (record.timestamp_us(), record.content_length() as u64))
        .collect::<Vec<_>>();
    let taken_records = std::mem::take(batch);
    let errors = visitor.visit(entry_name, taken_records).await?;
    if let Some((_, err)) = errors.into_iter().next() {
        return Err(err);
    }

    for (ts, len) in batch_info {
        *record_count += 1;
        *timestamp = ts;
        if let Ok(mut progress) = bucket_progress.lock() {
            progress.update(entry_name, len);
        }
        sleep(Duration::from_micros(5)).await;
    }

    *attempts = DOWNLOAD_ATTEMPTS; // reset attempts on success
    Ok(())
}

// Decrement attempts and retry on failure
// and start from the last timestamp
// we also need to adjust the limit if we have one
async fn make_attempt(
    attempts: &mut u8,
    bucket_progress: &Arc<Mutex<BucketProgress>>,
    params: &mut QueryParams,
    record_count: u64,
    timestamp: u64,
    err: ReductError,
) -> Result<(), ReductError> {
    *attempts -= 1;

    if *attempts == 0 {
        Err(err)
    } else {
        params.start = Some(timestamp);
        if let Some(limit) = params.limit {
            // if we have a limit, we need to reset it on retry
            params.limit = Some(limit.saturating_sub(record_count));
        }

        if let Ok(progress) = bucket_progress.lock() {
            progress.print_warning(format!(
                "{}. Retrying... (attempts {} / {})",
                err, *attempts, DOWNLOAD_ATTEMPTS
            ));
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    }
}

fn init_bucket_progress_bar(progress: &MultiProgress, quiet: bool) -> ProgressBar {
    if quiet {
        ProgressBar::hidden()
    } else {
        let sty = ProgressStyle::with_template(
            "[{elapsed_precise}, ETA {eta_precise}] {bar:40.green/gray} {percent_precise:6}% {msg}",
        )
        .unwrap();
        let local_progress = ProgressBar::new(1);
        let local_progress = progress.add(local_progress);
        local_progress.set_style(sty);
        local_progress
    }
}

#[cfg(test)]
mod tests {
    use rstest::*;

    use super::*;

    mod downloading {
        use super::*;
        use crate::context::tests::{bucket, context};
        use crate::context::CliContext;
        use crate::io::reduct::build_client;
        use async_trait::async_trait;
        use bytes::Bytes;
        use mockall::mock;
        use mockall::predicate::{always, eq};
        use reduct_rs::ErrorCode;

        mock! {
            pub Visitor {}
            #[async_trait]
            impl CopyVisitor for Visitor {
                async fn visit(&self, entry_name: &str, records: Vec<Record>)  -> Result<BTreeMap<u64, ReductError>, ReductError>;
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
            visitor
                .expect_visit()
                .times(2)
                .return_const(Ok(BTreeMap::new()));

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
                .return_const(Ok(BTreeMap::new()));
            visitor
                .expect_visit()
                .withf(|entry, _record| entry == "entry-2")
                .times(1)
                .return_const(Ok(BTreeMap::new()));
            visitor
                .expect_visit()
                .withf(|entry, records| {
                    entry == "entry-3"
                        && records[0].timestamp_us() == 1
                        && records[0].labels().get("key").unwrap() == "value"
                        && records[0].content_type() == "text/plain"
                })
                .times(1)
                .return_const(Ok(BTreeMap::new()));

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
                .return_const(Ok(BTreeMap::new()));

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
            visitor
                .expect_visit()
                .times(2)
                .return_const(Ok(BTreeMap::new()));

            let params = QueryParams {
                entry_filter: vec!["entry-*".to_string()],
                quiet: false,
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
                .return_const(Ok(BTreeMap::new()));

            let params = QueryParams {
                entry_filter: vec!["entry-1".to_string()],
                quiet: false,
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_limit(#[future] src_bucket: Bucket, mut visitor: MockVisitor) {
            let src_bucket = src_bucket.await;
            visitor
                .expect_visit()
                .times(2)
                .return_const(Ok(BTreeMap::new()));

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
                quiet: false,
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }
    }
}
