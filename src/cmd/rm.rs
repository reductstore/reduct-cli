// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::ALIAS_OR_URL_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::parse::widely_used_args::{
    make_each_n, make_each_s, make_entries_arg, make_exclude_arg, make_include_arg,
};
use crate::parse::{
    fetch_and_filter_entries, parse_query_params, parse_time, QueryParams, ResourcePathParser,
};
use async_trait::async_trait;
use clap::{Arg, Command};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reduct_rs::{Bucket, EntryInfo, RemoveQueryBuilder};
use serde::de::Visitor;
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
        .arg(make_include_arg())
        .arg(make_exclude_arg())
        .arg(make_entries_arg())
        .arg(make_each_n())
        .arg(make_each_s())
        .arg(
            Arg::new("time")
                .long("time")
                .short('T')
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

fn build_remover(
    query_params: QueryParams,
    timestamps: Option<Vec<String>>,
    bucket: Bucket,
) -> Arc<Box<dyn RemoveRecords + Send + Sync>> {
    let remover: Box<dyn RemoveRecords + Send + Sync> = if timestamps.is_some() {
        Box::new(BatchRemover {
            src_bucket: Arc::new(bucket),
            query_params: query_params.clone(),
            timestamps: timestamps.unwrap(),
        })
    } else {
        Box::new(QueryRemover {
            src_bucket: Arc::new(bucket),
            query_params: query_params.clone(),
        })
    };
    Arc::new(remover)
}

#[async_trait]
trait RemoveRecords {
    async fn remove_records(&self, entry: EntryInfo) -> anyhow::Result<u64>;
}

struct QueryRemover {
    src_bucket: Arc<Bucket>,
    query_params: QueryParams,
}

#[async_trait]
impl RemoveRecords for QueryRemover {
    async fn remove_records(&self, entry: EntryInfo) -> anyhow::Result<u64> {
        let query_builder = build_query(&self.src_bucket, &entry, &self.query_params);
        let removed_records = query_builder.send().await?;
        Ok(removed_records)
    }
}

struct BatchRemover {
    src_bucket: Arc<Bucket>,
    query_params: QueryParams,
    timestamps: Vec<String>,
}

#[async_trait]
impl RemoveRecords for BatchRemover {
    async fn remove_records(&self, entry: EntryInfo) -> anyhow::Result<u64> {
        let mut batch = self.src_bucket.remove_batch(&entry.name);
        for timestamp in &self.timestamps {
            batch.append_timestamp_us(parse_time(Some(timestamp))?.unwrap());
        }

        let error_map = batch.send().await?;
        Ok(self.timestamps.len() as u64 - error_map.len() as u64)
    }
}

fn build_query(
    src_bucket: &Bucket,
    entry: &EntryInfo,
    query_params: &QueryParams,
) -> RemoveQueryBuilder {
    let mut query_builder = src_bucket.remove_query(&entry.name);
    if let Some(start) = query_params.start {
        query_builder = query_builder.start_us(start as u64);
    }

    if let Some(stop) = query_params.stop {
        query_builder = query_builder.stop_us(stop as u64);
    }

    if let Some(each_n) = query_params.each_n {
        query_builder = query_builder.each_n(each_n);
    }

    if let Some(each_s) = query_params.each_s {
        query_builder = query_builder.each_s(each_s);
    }

    query_builder = query_builder.include(query_params.include_labels.clone());
    query_builder = query_builder.exclude(query_params.exclude_labels.clone());

    query_builder
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::context;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn folder_to_folder_unsupported(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "./", "./"])
            .unwrap();
        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn folder_to_bucket_unsupported(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "./", "local/bucket"])
            .unwrap();
        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
    }
}
