// Copyright 2023-2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::bucket::helpers::print_bucket_status;
use crate::cmd::table::{build_info_table, labeled_cell, record_range_cells};
use crate::cmd::ALIAS_OR_URL_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use bytesize::ByteSize;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::BucketInfoList;

pub(super) fn ls_bucket_cmd() -> Command {
    Command::new("ls")
        .about("List buckets")
        .arg(
            Arg::new("ALIAS_OR_URL")
                .help(ALIAS_OR_URL_HELP)
                .required(true),
        )
        .arg(
            Arg::new("full")
                .long("full")
                .short('f')
                .action(SetTrue)
                .help("Show detailed bucket information")
                .required(false),
        )
}

pub(super) async fn ls_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let alias_or_url = args.get_one::<String>("ALIAS_OR_URL").unwrap();
    let client = build_client(ctx, alias_or_url).await?;

    let bucket_list = client.bucket_list().await?;
    if args.get_flag("full") {
        print_full_list(ctx, bucket_list);
    } else {
        print_list(ctx, bucket_list);
    }
    Ok(())
}

fn print_list(ctx: &CliContext, bucket_list: BucketInfoList) {
    for bucket in bucket_list.buckets {
        output!(ctx, "{}", bucket.name);
    }
}

fn print_full_list(ctx: &CliContext, bucket_list: BucketInfoList) {
    let mut info_cells = Vec::new();
    let buckets = bucket_list.buckets;
    let total = buckets.len();

    for (idx, bucket) in buckets.into_iter().enumerate() {
        info_cells.push(labeled_cell("Name", bucket.name.clone()));
        info_cells.push(labeled_cell("Entries", bucket.entry_count));
        info_cells.push(labeled_cell(
            "Size",
            ByteSize(bucket.size).display().si().to_string(),
        ));
        info_cells.push(labeled_cell("Status", print_bucket_status(&bucket.status)));
        info_cells.extend(record_range_cells(
            bucket.oldest_record,
            bucket.latest_record,
            bucket.entry_count == 0,
        ));

        if idx + 1 < total {
            info_cells.push(String::new());
            info_cells.push(String::new());
        }
    }

    if info_cells.is_empty() {
        return;
    }

    let table = build_info_table(info_cells);
    output!(ctx, "{}", table);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, bucket2, context};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_ls_bucket(context: CliContext, #[future] bucket: String) {
        let args = ls_bucket_cmd().get_matches_from(vec!["ls", "local"]);
        build_client(&context, "local")
            .await
            .unwrap()
            .create_bucket(&bucket.await)
            .send()
            .await
            .unwrap();

        ls_bucket(&context, &args).await.unwrap();
        assert!(context
            .stdout()
            .history()
            .contains(&"test_bucket".to_string()));
    }

    #[rstest]
    #[tokio::test]
    async fn test_ls_bucket_full(
        context: CliContext,
        #[future] bucket: String,
        #[future] bucket2: String,
    ) {
        let args = ls_bucket_cmd().get_matches_from(vec!["ls", "local", "--full"]);
        let client = build_client(&context, "local").await.unwrap();
        let bucket = client.create_bucket(&bucket.await).send().await.unwrap();
        bucket
            .write_record("test")
            .data("data")
            .timestamp_us(0)
            .send()
            .await
            .unwrap();
        bucket
            .write_record("test")
            .data("data")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();
        client.create_bucket(&bucket2.await).send().await.unwrap();

        ls_bucket(&context, &args).await.unwrap();

        let mut expected_cells = vec![
            labeled_cell("Name", "test_bucket"),
            labeled_cell("Entries", 1),
            labeled_cell("Size", "74 B"),
            labeled_cell("Status", "Ready"),
        ];
        expected_cells.extend(record_range_cells(0, 1000, false));
        expected_cells.push(String::new());
        expected_cells.push(String::new());
        expected_cells.push(labeled_cell("Name", "test_bucket_2"));
        expected_cells.push(labeled_cell("Entries", 0));
        expected_cells.push(labeled_cell("Size", "0 B"));
        expected_cells.push(labeled_cell("Status", "Ready"));
        expected_cells.extend(record_range_cells(0, 0, true));

        let expected_table = build_info_table(expected_cells);

        assert_eq!(context.stdout().history(), vec![expected_table]);
    }
}
