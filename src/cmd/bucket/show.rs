// Copyright 2023-2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::bucket::helpers::print_bucket_status;
use crate::cmd::table::{build_info_table, labeled_cell, record_range_cells};
use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::helpers::timestamp_to_iso;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::ResourcePathParser;
use bytesize::ByteSize;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::{BucketInfo, EntryInfo, FullBucketInfo, ReductClient};
use tabled::{settings::Style, Table, Tabled};

pub(super) fn show_bucket_cmd() -> Command {
    Command::new("show")
        .about("Show bucket information")
        .arg(
            Arg::new("BUCKET_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("full")
                .long("full")
                .short('f')
                .action(SetTrue)
                .help("Show detailed bucket information with entries")
                .required(false),
        )
}

#[derive(Tabled)]
struct EntryTable {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Records")]
    record_count: u64,
    #[tabled(rename = "Blocks")]
    block_count: u64,

    #[tabled(rename = "Size")]
    size: String,
    #[tabled(rename = "Oldest Record (UTC)")]
    oldest_record: String,
    #[tabled(rename = "Latest Record (UTC)")]
    latest_record: String,
}

impl From<EntryInfo> for EntryTable {
    fn from(entry: EntryInfo) -> Self {
        Self {
            name: entry.name,
            record_count: entry.record_count,
            block_count: entry.block_count,
            size: ByteSize(entry.size).display().si().to_string(),
            oldest_record: timestamp_to_iso(entry.oldest_record, entry.record_count == 0),
            latest_record: timestamp_to_iso(entry.latest_record, entry.record_count == 0),
        }
    }
}

pub(super) async fn show_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, bucket_name) = args.get_one::<(String, String)>("BUCKET_PATH").unwrap();

    let client: ReductClient = build_client(ctx, alias_or_url).await?;
    let bucket = client.get_bucket(bucket_name).await?.full_info().await?;

    if args.get_flag("full") {
        print_full_bucket(ctx, bucket)?;
    } else {
        print_bucket(ctx, bucket.info)?;
    }

    Ok(())
}

fn print_bucket(ctx: &CliContext, bucket: BucketInfo) -> anyhow::Result<()> {
    let mut info_cells = vec![
        labeled_cell("Name", bucket.name),
        labeled_cell("Entries", bucket.entry_count),
        labeled_cell("Size", ByteSize(bucket.size).display().si().to_string()),
        labeled_cell("Status", print_bucket_status(&bucket.status)),
    ];
    info_cells.extend(record_range_cells(
        bucket.oldest_record,
        bucket.latest_record,
        bucket.entry_count == 0,
    ));

    let info_table = build_info_table(info_cells);

    output!(ctx, "{}", info_table);
    output!(ctx, "");
    Ok(())
}

fn print_full_bucket(ctx: &CliContext, bucket: FullBucketInfo) -> anyhow::Result<()> {
    let settings = bucket.settings;
    let info = bucket.info;
    let mut info_cells = vec![
        labeled_cell("Name", info.name),
        labeled_cell("Quota Type", settings.quota_type.unwrap()),
        labeled_cell("Entries", info.entry_count),
        labeled_cell(
            "Quota Size",
            ByteSize(settings.quota_size.unwrap())
                .display()
                .si()
                .to_string(),
        ),
        labeled_cell("Size", ByteSize(info.size).display().si().to_string()),
        labeled_cell(
            "Max. Block Size",
            ByteSize(settings.max_block_size.unwrap())
                .display()
                .si()
                .to_string(),
        ),
        labeled_cell("Status", print_bucket_status(&info.status)),
        labeled_cell("Max. Block Records", settings.max_block_records.unwrap()),
    ];
    info_cells.extend(record_range_cells(
        info.oldest_record,
        info.latest_record,
        info.entry_count == 0,
    ));

    let info_table = build_info_table(info_cells);

    output!(ctx, "{}", info_table);
    output!(ctx, "");

    let entries = bucket.entries.into_iter().map(EntryTable::from);
    let table = Table::new(entries).with(Style::markdown()).to_string();
    output!(ctx, "{}", table);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, context};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_show_bucket(context: CliContext, #[future] bucket: String) {
        let bucket_name = bucket.await;
        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket_name).send().await.unwrap();

        let args = show_bucket_cmd()
            .get_matches_from(vec!["show", format!("local/{}", bucket_name).as_str()]);

        assert_eq!(show_bucket(&context, &args).await.unwrap(), ());
        let mut expected_cells = vec![
            labeled_cell("Name", "test_bucket"),
            labeled_cell("Entries", 0),
            labeled_cell("Size", "0 B"),
            labeled_cell("Status", "Ready"),
        ];
        expected_cells.extend(record_range_cells(0, 0, true));
        let expected_info_table = build_info_table(expected_cells);

        assert_eq!(
            context.stdout().history(),
            vec![expected_info_table, String::new()]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_show_bucket_full(context: CliContext, #[future] bucket: String) {
        let bucket_name = bucket.await;
        let client = build_client(&context, "local").await.unwrap();
        let bucket = client.create_bucket(&bucket_name).send().await.unwrap();
        bucket
            .write_record("test")
            .data("data")
            .timestamp_us(1)
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

        let args = show_bucket_cmd().get_matches_from(vec![
            "show",
            format!("local/{}", bucket_name).as_str(),
            "--full",
        ]);

        assert_eq!(show_bucket(&context, &args).await.unwrap(), ());
        let mut expected_cells = vec![
            labeled_cell("Name", "test_bucket"),
            labeled_cell("Quota Type", "NONE"),
            labeled_cell("Entries", 1),
            labeled_cell("Quota Size", "0 B"),
            labeled_cell("Size", "77 B"),
            labeled_cell("Max. Block Size", "64.0 MB"),
            labeled_cell("Status", "Ready"),
            labeled_cell("Max. Block Records", 1024),
        ];
        expected_cells.extend(record_range_cells(0, 1000, false));
        let expected_info_table = build_info_table(expected_cells);

        assert_eq!(
            context.stdout().history(),
            vec![
                expected_info_table,
                String::new(),
                "| Name | Records | Blocks | Size | Oldest Record (UTC)      | Latest Record (UTC)      |\n|------|---------|--------|------|--------------------------|--------------------------|\n| test | 2       | 1      | 77 B | 1970-01-01T00:00:00.000Z | 1970-01-01T00:00:00.001Z |"
                    .to_string(),
            ]
        );
    }
}
