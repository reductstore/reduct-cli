// Copyright 2023-2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::bucket::helpers::print_bucket_status;
use crate::cmd::ALIAS_OR_URL_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use bytesize::ByteSize;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::BucketInfoList;
use tabled::settings::Style;
use tabled::{Table, Tabled};

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

#[derive(Tabled)]
struct BucketRow {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Entries")]
    entries: u64,
    #[tabled(rename = "Size")]
    size: String,
    #[tabled(rename = "Status")]
    status: String,
    #[tabled(rename = "Provisioned")]
    provisioned: String,
    #[tabled(rename = "Oldest Record (UTC)")]
    oldest_record: String,
    #[tabled(rename = "Latest Record (UTC)")]
    latest_record: String,
}

fn record_range_values(oldest: u64, latest: u64, is_empty: bool) -> (String, String) {
    let cells = crate::cmd::table::record_range_cells(oldest, latest, is_empty);
    let oldest_value = cells
        .get(0)
        .and_then(|cell| cell.split_once(": "))
        .map(|(_, value)| value.to_string())
        .unwrap_or_default();
    let latest_value = cells
        .get(2)
        .and_then(|cell| cell.split_once(": "))
        .map(|(_, value)| value.to_string())
        .unwrap_or_default();
    (oldest_value, latest_value)
}

fn print_full_list(ctx: &CliContext, bucket_list: BucketInfoList) {
    if bucket_list.buckets.is_empty() {
        return;
    }

    let rows = bucket_list
        .buckets
        .into_iter()
        .map(|bucket| {
            let (oldest_record, latest_record) = record_range_values(
                bucket.oldest_record,
                bucket.latest_record,
                bucket.entry_count == 0,
            );
            BucketRow {
                name: bucket.name,
                entries: bucket.entry_count,
                size: ByteSize(bucket.size).display().si().to_string(),
                status: print_bucket_status(&bucket.status),
                provisioned: if bucket.is_provisioned {
                    "✓".to_string()
                } else {
                    "-".to_string()
                },
                oldest_record,
                latest_record,
            }
        })
        .collect::<Vec<_>>();

    let table = Table::new(rows).with(Style::markdown()).to_string();
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

        let (oldest_record, latest_record) = record_range_values(0, 1000, false);
        let (oldest_record_2, latest_record_2) = record_range_values(0, 0, true);
        let expected_rows = vec![
            BucketRow {
                name: "test_bucket".to_string(),
                entries: 1,
                size: "74 B".to_string(),
                status: "✅ Ready".to_string(),
                provisioned: "-".to_string(),
                oldest_record,
                latest_record,
            },
            BucketRow {
                name: "test_bucket_2".to_string(),
                entries: 0,
                size: "0 B".to_string(),
                status: "✅ Ready".to_string(),
                provisioned: "-".to_string(),
                oldest_record: oldest_record_2,
                latest_record: latest_record_2,
            },
        ];
        let expected_table = Table::new(expected_rows)
            .with(Style::markdown())
            .to_string();

        assert_eq!(context.stdout().history(), vec![expected_table]);
    }
}
