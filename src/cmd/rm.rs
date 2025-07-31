// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod batch_remover;
mod query_remover;

use crate::cmd::rm::batch_remover::BatchRemover;
use crate::cmd::rm::query_remover::QueryRemover;
use crate::cmd::ALIAS_OR_URL_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::parse::widely_used_args::{
    make_each_n, make_each_s, make_entries_arg,
    make_strict_arg, make_when_arg,
};
use crate::parse::{fetch_and_filter_entries, parse_query_params, QueryParams, ResourcePathParser};
use async_trait::async_trait;
use clap::{Arg, Command};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reduct_rs::{Bucket, EntryInfo};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

pub(crate) fn rm_cmd() -> Command {
    Command::new("rm")
        .about("Remove data from a bucket")
        .arg_required_else_help(true)
        .arg(
            Arg::new("BUCKET_PATH")
                .help(ALIAS_OR_URL_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("start")
                .long("start")
                .short('b')
                .help("Remove records  with timestamps older than this time point in ISO format or Unix timestamp in microseconds.\nIf not specified, the export will start from the first record in an entry.")
                .required(false)
        )
        .arg(
            Arg::new("stop")
                .long("stop")
                .short('e')
                .help("Remove records with timestamps newer than this time point in ISO format or Unix timestamp in microseconds.\nIf not specified, the export will end at the last record in an entry.")
                .required(false)
        )
        .arg(make_entries_arg())
        .arg(make_each_n())
        .arg(make_each_s())
        .arg(make_when_arg())
        .arg(make_strict_arg())
        .arg(
            Arg::new("time")
                .long("time")
                .short('t')
                .value_name("TIMESTAMP")
                .help("Remove a record with a certain timestamp in ISO format or Unix timestamp in microseconds.")
                .required(false)
                .num_args(0..)
        )
}

pub(crate) async fn rm_handler(ctx: &CliContext, args: &clap::ArgMatches) -> anyhow::Result<()> {
    let (alias, bucket) = args.get_one::<(String, String)>("BUCKET_PATH").unwrap();
    let query_params = parse_query_params(ctx, &args)?;
    let timestamps = args
        .get_many::<String>("time")
        .map(|values| values.map(|s| s.clone()).collect::<Vec<String>>());

    let client = build_client(ctx, &alias).await?;
    let bucket = client.get_bucket(&bucket).await?;
    let entries = fetch_and_filter_entries(&bucket, &query_params.entry_filter).await?;

    let mut tasks = JoinSet::new();
    let progress = MultiProgress::new();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(query_params.parallel));
    let remover = build_remover(query_params, timestamps, bucket);

    for entry in entries {
        let local_sem = Arc::clone(&semaphore);
        let local_remover = Arc::clone(&remover);

        let spinner = progress.add(ProgressBar::new_spinner());
        let entry_name = entry.name.clone();
        tasks.spawn(async move {
            spinner.enable_steady_tick(Duration::from_millis(120));
            spinner.set_style(
                ProgressStyle::with_template("[{elapsed_precise}] {spinner:.green} {msg}")
                    .unwrap()
                    // For more spinners check out the cli-spinners project:
                    // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
                    .tick_strings(&["▁", "▃", "▄", "▅", "▆", "▇"]),
            );
            spinner.set_message(format!("Removing records from '{}'", entry_name));

            let _permit = local_sem.acquire().await.unwrap();
            match local_remover.remove_records(entry).await {
                Ok(removed_records) => {
                    spinner.finish_with_message(format!(
                        "Removed {} records from '{}'",
                        removed_records, entry_name
                    ));
                }
                Err(err) => {
                    spinner.finish_with_message(format!(
                        "Failed to remove records from '{}': {}",
                        entry_name, err
                    ));
                }
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        let _ = result?;
    }
    Ok(())
}

/// Trait for removing records from a bucket
#[async_trait]
trait RemoveRecords {
    async fn remove_records(&self, entry: EntryInfo) -> anyhow::Result<u64>;
}

/// Build a remover based on the query parameters
/// If timestamps are provided, a BatchRemover will be created
fn build_remover(
    query_params: QueryParams,
    timestamps: Option<Vec<String>>,
    bucket: Bucket,
) -> Arc<Box<dyn RemoveRecords + Send + Sync>> {
    let remover: Box<dyn RemoveRecords + Send + Sync> = if timestamps.is_some() {
        Box::new(BatchRemover::new(bucket, timestamps.unwrap()))
    } else {
        Box::new(QueryRemover::new(bucket, query_params))
    };
    Arc::new(remover)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, context};
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_remove_by_query(
        context: CliContext,
        #[future] bucket: String,
        #[future] bucket_with_record: Bucket,
    ) {
        let args = rm_cmd().get_matches_from(vec![
            "rm",
            &format!("local/{}", bucket.await),
            "--start",
            "100",
            "--stop",
            "200",
        ]);
        let bucket = bucket_with_record.await;

        rm_handler(&context, &args).await.unwrap();

        assert_eq!(
            bucket
                .read_record("entry-1")
                .timestamp_us(150)
                .send()
                .await
                .err()
                .unwrap()
                .to_string(),
            "[NotFound] No record with timestamp 150"
        );
        assert_eq!(
            bucket
                .read_record("entry-2")
                .timestamp_us(150)
                .send()
                .await
                .err()
                .unwrap()
                .to_string(),
            "[NotFound] No record with timestamp 150"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_in_batch(
        context: CliContext,
        #[future] bucket: String,
        #[future] bucket_with_record: Bucket,
    ) {
        let args = rm_cmd().get_matches_from(vec![
            "rm",
            &format!("local/{}", bucket.await),
            "--time",
            "150",
            "100",
        ]);
        let bucket = bucket_with_record.await;

        rm_handler(&context, &args).await.unwrap();

        assert_eq!(
            bucket
                .read_record("entry-1")
                .timestamp_us(150)
                .send()
                .await
                .err()
                .unwrap()
                .to_string(),
            "[NotFound] No record with timestamp 150"
        );
        assert_eq!(
            bucket
                .read_record("entry-2")
                .timestamp_us(150)
                .send()
                .await
                .err()
                .unwrap()
                .to_string(),
            "[NotFound] No record with timestamp 150"
        );
    }

    #[fixture]
    pub(super) async fn bucket_with_record(
        context: CliContext,
        #[future] bucket: String,
    ) -> Bucket {
        let client = build_client(&context, "local").await.unwrap();
        let bucket = client.create_bucket(&bucket.await).send().await.unwrap();
        bucket
            .write_record("entry-1")
            .data("data")
            .timestamp_us(150)
            .send()
            .await
            .unwrap();
        bucket
            .write_record("entry-2")
            .data("data")
            .timestamp_us(150)
            .send()
            .await
            .unwrap();
        bucket
    }
}
