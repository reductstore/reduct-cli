// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::rm::RemoveRecords;
use crate::parse::parse_time;
use async_trait::async_trait;
use reduct_rs::{Bucket, EntryInfo};

/// Remove records from a bucket using a list of timestamps
pub(super) struct BatchRemover {
    bucket: Bucket,
    timestamps: Vec<String>,
}

#[async_trait]
impl RemoveRecords for BatchRemover {
    async fn remove_records(&self, entry: EntryInfo) -> anyhow::Result<u64> {
        let mut batch = self.bucket.remove_batch(&entry.name);
        for timestamp in &self.timestamps {
            batch.append_timestamp_us(parse_time(Some(timestamp))?.unwrap());
        }

        let error_map = batch.send().await?;
        Ok(self.timestamps.len() as u64 - error_map.len() as u64)
    }
}
impl BatchRemover {
    pub fn new(bucket: Bucket, timestamps: Vec<String>) -> Self {
        Self { bucket, timestamps }
    }
}

#[cfg(test)]
mod tests {
    use crate::cmd::rm::batch_remover::BatchRemover;
    use crate::cmd::rm::tests::bucket_with_record;
    use crate::cmd::rm::RemoveRecords;
    use reduct_rs::{Bucket, EntryInfo};
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_batch_remover(#[future] bucket_with_record: Bucket) {
        let bucket = bucket_with_record.await;

        let query_remover = BatchRemover::new(bucket, vec!["100".to_string(), "150".to_string()]);
        let entry = EntryInfo {
            name: "entry-1".to_string(),
            size: 0,
            record_count: 0,
            block_count: 0,
            oldest_record: 0,
            latest_record: 0,
            status: Default::default(),
        };

        assert_eq!(query_remover.remove_records(entry).await.unwrap(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_batch_remover_iso(#[future] bucket_with_record: Bucket) {
        let bucket = bucket_with_record.await;

        let query_remover = BatchRemover::new(
            bucket,
            vec!["150".to_string(), "2024-01-01T10:00:00Z".to_string()],
        );
        let entry = EntryInfo {
            name: "entry-1".to_string(),
            size: 0,
            record_count: 0,
            block_count: 0,
            oldest_record: 0,
            latest_record: 0,
            status: Default::default(),
        };

        assert_eq!(query_remover.remove_records(entry).await.unwrap(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_batch_wrong_time_format(#[future] bucket_with_record: Bucket) {
        let bucket = bucket_with_record.await;

        let query_remover = BatchRemover::new(bucket, vec!["xxx".to_string()]);
        let entry = EntryInfo {
            name: "entry-1".to_string(),
            size: 0,
            record_count: 0,
            block_count: 0,
            oldest_record: 0,
            latest_record: 0,
            status: Default::default(),
        };

        assert_eq!(
            query_remover
                .remove_records(entry)
                .await
                .err()
                .unwrap()
                .to_string(),
            "Failed to parse time xxx: premature end of input"
        );
    }
}
