// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::cp::helpers::{start_loading_with_entry_start_overrides, CopyVisitor};
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::parse::parse_query_params;
use clap::ArgMatches;
use reduct_rs::{Bucket, ErrorCode, Record, ReductError};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

struct CopyToBucketVisitor {
    dst_bucket: Arc<Bucket>,
}

#[async_trait::async_trait]
impl CopyVisitor for CopyToBucketVisitor {
    async fn visit(
        &self,
        entry_name: &str,
        mut records: Vec<Record>,
    ) -> Result<BTreeMap<u64, ReductError>, ReductError> {
        if records.len() == 1 {
            let mut result: BTreeMap<u64, ReductError> = BTreeMap::new();
            let record = records.pop().unwrap();
            let timestamp = record.timestamp_us();
            let res = self
                .dst_bucket
                .write_record(entry_name)
                .timestamp_us(record.timestamp_us())
                .labels(record.labels().clone())
                .content_type(record.content_type())
                .content_length(record.content_length() as u64)
                .stream(record.stream_bytes())
                .send()
                .await;
            if let Err(err) = res {
                if err.status() != ErrorCode::Conflict {
                    result.insert(timestamp, err);
                }
            }
            Ok(result)
        } else {
            let errors = self
                .dst_bucket
                .write_batch(entry_name)
                .add_records(records)
                .send()
                .await?;
            Ok(errors
                .into_iter()
                .filter(|(_, err)| err.status() != ErrorCode::Conflict)
                .collect())
        }
    }
}

pub(crate) async fn cp_bucket_to_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (src_instance, src_bucket) = args
        .get_one::<(String, String)>("SOURCE_BUCKET_OR_FOLDER")
        .map(|(src_instance, src_bucket)| (src_instance.clone(), src_bucket.clone()))
        .unwrap();
    let (dst_instance, dst_bucket) = args
        .get_one::<(String, String)>("DESTINATION_BUCKET_OR_FOLDER")
        .map(|(dst_instance, dst_bucket)| (dst_instance.clone(), dst_bucket.clone()))
        .unwrap();

    let query_params = parse_query_params(ctx, &args)?;
    let from_last = args.get_flag("from-last");
    if from_last && args.get_one::<String>("start").is_some() {
        return Err(anyhow::anyhow!("--from-last can't be used with --start"));
    }

    let src_bucket = build_client(ctx, &src_instance)
        .await?
        .get_bucket(&src_bucket)
        .await?;

    let dst_client = build_client(ctx, &dst_instance).await?;
    let dst_bucket = match dst_client.get_bucket(&dst_bucket).await {
        Ok(bucket) => bucket,
        Err(err) => {
            if err.status() == ErrorCode::NotFound {
                // Create the bucket if it does not exist with the same settings as the source bucket
                dst_client
                    .create_bucket(&dst_bucket)
                    .settings(src_bucket.settings().await?)
                    .send()
                    .await?
            } else {
                return Err(err.into());
            }
        }
    };

    let dst_bucket_visitor = CopyToBucketVisitor {
        dst_bucket: Arc::new(dst_bucket),
    };

    let entry_start_overrides = if from_last {
        Some(build_entry_start_overrides(dst_bucket_visitor.dst_bucket.as_ref()).await?)
    } else {
        None
    };

    start_loading_with_entry_start_overrides(
        src_bucket,
        query_params,
        entry_start_overrides,
        dst_bucket_visitor,
    )
    .await
}

async fn build_entry_start_overrides(dst_bucket: &Bucket) -> anyhow::Result<HashMap<String, u64>> {
    let mut overrides = HashMap::new();
    for entry in dst_bucket.entries().await? {
        let start = entry.latest_record.saturating_add(1);
        overrides.insert(entry.name, start);
    }
    Ok(overrides)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::cmd::cp::cp_cmd;
    use crate::context::tests::{bucket, bucket2, context};
    use bytes::Bytes;
    use reduct_rs::{QuotaType, RecordBuilder};
    use rstest::{fixture, rstest};

    use super::*;

    mod copy_visitor {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_visit(context: CliContext, #[future] bucket: String, records: Vec<Record>) {
            let client = build_client(&context, "local").await.unwrap();
            let dst_bucket = client
                .create_bucket(&bucket.await)
                .exist_ok(true)
                .send()
                .await
                .unwrap();

            let dst_bucket = Arc::new(dst_bucket);
            let visitor = CopyToBucketVisitor {
                dst_bucket: Arc::clone(&dst_bucket),
            };

            // should write the record to the destination bucket
            visitor.visit("test", records).await.unwrap();

            let record = dst_bucket
                .read_record("test")
                .timestamp_us(123456)
                .send()
                .await
                .unwrap();
            assert_eq!(record.content_type(), "text/plain");
            assert_eq!(record.content_length(), 4);
            assert_eq!(record.bytes().await.unwrap(), Bytes::from_static(b"test"));
        }

        #[rstest]
        #[tokio::test]
        async fn test_visit_with_batch(
            context: CliContext,
            #[future] bucket: String,
            batch_records: Vec<Record>,
        ) {
            let client = build_client(&context, "local").await.unwrap();
            let dst_bucket = client
                .create_bucket(&bucket.await)
                .exist_ok(true)
                .send()
                .await
                .unwrap();

            let dst_bucket = Arc::new(dst_bucket);
            let visitor = CopyToBucketVisitor {
                dst_bucket: Arc::clone(&dst_bucket),
            };

            // should write the record to the destination bucket
            visitor.visit("batch_test", batch_records).await.unwrap();

            let record = dst_bucket
                .read_record("batch_test")
                .timestamp_us(123457)
                .send()
                .await
                .unwrap();
            assert_eq!(record.content_type(), "text/plain");
            assert_eq!(record.content_length(), 5);
            assert_eq!(record.bytes().await.unwrap(), Bytes::from_static(b"test1"));

            let record = dst_bucket
                .read_record("batch_test")
                .timestamp_us(123458)
                .send()
                .await
                .unwrap();
            assert_eq!(record.content_type(), "text/plain");
            assert_eq!(record.content_length(), 5);
            assert_eq!(record.bytes().await.unwrap(), Bytes::from_static(b"test2"));
        }
    }

    mod cp_bucket_to_bucket {
        use super::*;
        use crate::cmd::cp::cp_cmd;

        #[rstest]
        #[tokio::test]
        async fn test_cp_bucket_to_bucket(
            context: CliContext,
            #[future] bucket: String,
            #[future] bucket2: String,
        ) {
            let client = build_client(&context, "local").await.unwrap();
            let src_bucket = client.create_bucket(&bucket.await).send().await.unwrap();
            let dst_bucket = client.create_bucket(&bucket2.await).send().await.unwrap();

            let test_data = Bytes::from_static(b"test");

            src_bucket
                .write_record("test")
                .timestamp_us(123456)
                .data(test_data.clone())
                .send()
                .await
                .unwrap();

            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    format!("local/{}", src_bucket.name()).as_str(),
                    format!("local/{}", dst_bucket.name()).as_str(),
                ])
                .unwrap();

            cp_bucket_to_bucket(&context, &args).await.unwrap();

            let record = dst_bucket
                .read_record("test")
                .timestamp_us(123456)
                .send()
                .await
                .unwrap();
            assert_eq!(record.bytes().await.unwrap(), test_data);
        }
        #[rstest]
        #[tokio::test]
        async fn test_cp_bucket_to_bucket_more_than_80_records(
            context: CliContext,
            #[future] bucket: String,
            #[future] bucket2: String,
        ) {
            let client = build_client(&context, "local").await.unwrap();
            let src_bucket = client.create_bucket(&bucket.await).send().await.unwrap();
            let dst_bucket = client.create_bucket(&bucket2.await).send().await.unwrap();

            for i in 0..81 {
                let test_data = Bytes::from(format!("test{}", i));
                src_bucket
                    .write_record("test")
                    .timestamp_us(123456 + i)
                    .data(test_data.clone())
                    .send()
                    .await
                    .unwrap();
            }

            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    format!("local/{}", src_bucket.name()).as_str(),
                    format!("local/{}", dst_bucket.name()).as_str(),
                ])
                .unwrap();

            cp_bucket_to_bucket(&context, &args).await.unwrap();

            let record = dst_bucket
                .read_record("test")
                .timestamp_us(123456)
                .send()
                .await
                .unwrap();
            assert_eq!(record.bytes().await.unwrap(), Bytes::from_static(b"test0"));
            let record = dst_bucket
                .read_record("test")
                .timestamp_us(123496)
                .send()
                .await
                .unwrap();
            assert_eq!(record.bytes().await.unwrap(), Bytes::from_static(b"test40"));
            let record = dst_bucket
                .read_record("test")
                .timestamp_us(123536)
                .send()
                .await
                .unwrap();
            assert_eq!(record.bytes().await.unwrap(), Bytes::from_static(b"test80"));
        }

        #[rstest]
        #[tokio::test]
        async fn test_cp_bucket_to_bucket_from_last_with_limit(
            context: CliContext,
            #[future] bucket: String,
            #[future] bucket2: String,
        ) {
            let client = build_client(&context, "local").await.unwrap();
            let src_bucket = client.create_bucket(&bucket.await).send().await.unwrap();
            let dst_bucket = client.create_bucket(&bucket2.await).send().await.unwrap();

            src_bucket
                .write_record("test")
                .timestamp_us(100)
                .data(Bytes::from_static(b"src-old"))
                .send()
                .await
                .unwrap();
            src_bucket
                .write_record("test")
                .timestamp_us(200)
                .data(Bytes::from_static(b"src-new"))
                .send()
                .await
                .unwrap();

            dst_bucket
                .write_record("test")
                .timestamp_us(100)
                .data(Bytes::from_static(b"dst-old"))
                .send()
                .await
                .unwrap();

            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    format!("local/{}", src_bucket.name()).as_str(),
                    format!("local/{}", dst_bucket.name()).as_str(),
                    "--from-last",
                    "--limit",
                    "1",
                ])
                .unwrap();

            cp_bucket_to_bucket(&context, &args).await.unwrap();

            let record = dst_bucket
                .read_record("test")
                .timestamp_us(200)
                .send()
                .await
                .unwrap();
            assert_eq!(
                record.bytes().await.unwrap(),
                Bytes::from_static(b"src-new")
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_cp_bucket_to_bucket_from_last_with_start_rejected(
            context: CliContext,
            #[future] bucket: String,
            #[future] bucket2: String,
        ) {
            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    format!("local/{}", bucket.await).as_str(),
                    format!("local/{}", bucket2.await).as_str(),
                    "--from-last",
                    "--start",
                    "10",
                ])
                .unwrap();

            let result = cp_bucket_to_bucket(&context, &args).await;
            assert!(result.is_err());
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_cp_bucket_to_create_dst_bucket(
        context: CliContext,
        #[future] bucket: String,
        #[future] bucket2: String,
    ) {
        let dst_bucket = bucket2.await;
        let client = build_client(&context, "local").await.unwrap();
        let src_bucket = client
            .create_bucket(&bucket.await)
            .quota_type(QuotaType::FIFO)
            .send()
            .await
            .unwrap();

        let args = cp_cmd()
            .try_get_matches_from(vec![
                "cp",
                format!("local/{}", src_bucket.name()).as_str(),
                format!("local/{}", dst_bucket).as_str(),
            ])
            .unwrap();

        cp_bucket_to_bucket(&context, &args).await.unwrap();

        let dst_bucket = client.get_bucket("test_bucket").await.unwrap();
        assert_eq!(
            dst_bucket.settings().await.unwrap().quota_type,
            Some(QuotaType::FIFO),
            "The destination bucket should have the same settings as the source bucket"
        );
    }

    #[fixture]
    fn records() -> Vec<Record> {
        vec![RecordBuilder::new()
            .timestamp_us(123456)
            .content_type("text/plain".to_string())
            .add_label("key".to_string(), "value".to_string())
            .data(Bytes::from_static(b"test"))
            .build()]
    }

    #[fixture]
    fn batch_records() -> Vec<Record> {
        vec![
            RecordBuilder::new()
                .timestamp_us(123457)
                .content_type("text/plain".to_string())
                .add_label("key".to_string(), "value".to_string())
                .data(Bytes::from_static(b"test1"))
                .build(),
            RecordBuilder::new()
                .timestamp_us(123458)
                .content_type("text/plain".to_string())
                .add_label("key".to_string(), "value".to_string())
                .data(Bytes::from_static(b"test2"))
                .build(),
        ]
    }
}
