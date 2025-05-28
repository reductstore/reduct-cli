// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::rm::RemoveRecords;
use crate::parse::QueryParams;
use async_trait::async_trait;
use reduct_rs::{Bucket, EntryInfo, RemoveQueryBuilder};

/// Remove records from a bucket using a query
pub(super) struct QueryRemover {
    bucket: Bucket,
    query_params: QueryParams,
}

#[async_trait]
impl RemoveRecords for QueryRemover {
    async fn remove_records(&self, entry: EntryInfo) -> anyhow::Result<u64> {
        let query_builder = self.build_query(entry);
        let removed_records = query_builder.send().await?;
        Ok(removed_records)
    }
}

impl QueryRemover {
    pub fn new(bucket: Bucket, query_params: QueryParams) -> Self {
        Self {
            bucket,
            query_params,
        }
    }

    fn build_query(&self, entry: EntryInfo) -> RemoveQueryBuilder {
        let mut query_builder = self.bucket.remove_query(&entry.name);
        if let Some(start) = self.query_params.start {
            query_builder = query_builder.start_us(start as u64);
        }

        if let Some(stop) = self.query_params.stop {
            query_builder = query_builder.stop_us(stop as u64);
        }

        if let Some(each_n) = self.query_params.each_n {
            query_builder = query_builder.each_n(each_n);
        }

        if let Some(each_s) = self.query_params.each_s {
            query_builder = query_builder.each_s(each_s);
        }

        query_builder = query_builder.include(self.query_params.include_labels.clone());
        query_builder = query_builder.exclude(self.query_params.exclude_labels.clone());

        if let Some(when) = &self.query_params.when {
            query_builder = query_builder.when(when.clone());
        }

        query_builder = query_builder.strict(self.query_params.strict);

        query_builder
    }
}

#[cfg(test)]
mod tests {
    use crate::cmd::rm::query_remover::QueryRemover;
    use crate::cmd::rm::tests::bucket_with_record;
    use crate::cmd::rm::RemoveRecords;

    use crate::parse::QueryParams;
    use reduct_rs::{Bucket, EntryInfo};
    use rstest::*;
    use std::collections::HashMap;

    #[rstest]
    #[tokio::test]
    async fn test_query_remover(#[future] bucket_with_record: Bucket) {
        let bucket = bucket_with_record.await;
        let query_params = QueryParams {
            start: Some(100),
            stop: Some(200),
            each_n: None,
            each_s: None,
            limit: None,
            entry_filter: vec![],
            parallel: 0,
            include_labels: HashMap::new(),
            exclude_labels: HashMap::new(),
            ttl: Default::default(),
            when: None,
            strict: false,
            ext: None,
        };

        let query_remover = QueryRemover::new(bucket, query_params);
        let entry = EntryInfo {
            name: "entry-1".to_string(),
            size: 0,
            record_count: 0,
            block_count: 0,
            oldest_record: 0,
            latest_record: 0,
        };

        assert_eq!(query_remover.remove_records(entry).await.ok(), Some(1));
    }
}
