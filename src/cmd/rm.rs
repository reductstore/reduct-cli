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
use clap::ArgAction::SetTrue;
use clap::{value_parser, Arg, Command};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reduct_rs::{Bucket, EntryInfo, QueryBuilder, RemoveQueryBuilder, RemoveRecordBuilder};
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
}

pub(crate) async fn rm_handler(ctx: &CliContext, args: &clap::ArgMatches) -> anyhow::Result<()> {
    let (alias, bucket) = args.get_one::<(String, String)>("BUCKET_PATH").unwrap();

    let query_params = parse_query_params(ctx, &args)?;

    let client = build_client(ctx, &alias).await?;
    let bucket = client.get_bucket(&bucket).await?;
    let entries = fetch_and_filter_entries(&bucket, &query_params.entry_filter).await?;

    let mut tasks = JoinSet::new();
    let progress = MultiProgress::new();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(query_params.parallel));

    for entry in entries {
        let mut query_builder = build_query(&bucket, &entry, &query_params);
        let local_sem = Arc::clone(&semaphore);

        let spinner = progress.add(ProgressBar::new_spinner());

        tasks.spawn(async move {
            spinner.enable_steady_tick(Duration::from_millis(120));
            spinner.set_style(
                ProgressStyle::with_template("[{elapsed_precise}] {spinner:.green} {msg}")
                    .unwrap()
                    // For more spinners check out the cli-spinners project:
                    // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
                    .tick_strings(&["▁", "▃", "▄", "▅", "▆", "▇"]),
            );
            spinner.set_message(format!("Removing records from '{}'", entry.name));

            let _permit = local_sem.acquire().await.unwrap();
            match query_builder.send().await {
                Ok(removed_records) => {
                    spinner.finish_with_message(format!(
                        "Removed {} records from '{}'",
                        removed_records, entry.name
                    ));
                }
                Err(err) => {
                    spinner.finish_with_message(format!(
                        "Failed to remove records from '{}': {}",
                        entry.name, err
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
