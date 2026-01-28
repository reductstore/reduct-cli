// Copyright 2024-2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::replica::helpers::print_replication_mode;
use crate::cmd::table::{build_info_table, labeled_cell};
use crate::cmd::RESOURCE_PATH_HELP;
use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::{Arg, Command};
use tabled::settings::Style;
use tabled::{Table, Tabled};

pub(super) fn show_replica_cmd() -> Command {
    Command::new("show")
        .about("Show details about a replication task")
        .arg(
            Arg::new("REPLICATION_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(crate::parse::ResourcePathParser::new())
                .required(true),
        )
}

#[derive(Tabled)]
struct ErrorRow {
    #[tabled(rename = "Error Code")]
    code: i16,
    #[tabled(rename = "Count")]
    count: u64,
    #[tabled(rename = "Last Message")]
    message: String,
}

pub(super) async fn show_replica_handler(
    ctx: &crate::context::CliContext,
    args: &clap::ArgMatches,
) -> anyhow::Result<()> {
    let (alias_or_url, replication_name) = args
        .get_one::<(String, String)>("REPLICATION_PATH")
        .unwrap();
    let client = build_client(ctx, alias_or_url).await?;

    let replica = client.get_replication(replication_name).await?;

    let mut info_cells = vec![
        labeled_cell("Name", replica.info.name.clone()),
        labeled_cell("Active", replica.info.is_active),
        labeled_cell("Mode", print_replication_mode(replica.info.mode)),
        labeled_cell("Provisioned", replica.info.is_provisioned),
        labeled_cell("Ok Records (hourly)", replica.diagnostics.hourly.ok),
        labeled_cell("Errors (hourly)", replica.diagnostics.hourly.errored),
        labeled_cell("Source Bucket", replica.settings.src_bucket.clone()),
        labeled_cell("Destination Bucket", replica.settings.dst_bucket.clone()),
        labeled_cell("Destination Server", replica.settings.dst_host.clone()),
        labeled_cell("Entries", format!("{:?}", replica.settings.entries)),
    ];

    let when_value = replica
        .settings
        .when
        .as_ref()
        .map(|value| serde_json::to_string_pretty(value))
        .transpose()?;
    let when_cell_value = when_value
        .map(|value| value.replace('\n', "\n        "))
        .unwrap_or_else(|| "None".to_string());
    info_cells.push(labeled_cell("When", when_cell_value));

    let info_table = build_info_table(info_cells);
    output!(ctx, "{}", info_table);
    output!(ctx, "");

    let table = Table::new(
        replica
            .diagnostics
            .hourly
            .errors
            .into_iter()
            .map(|(code, err)| ErrorRow {
                code,
                count: err.count,
                message: err.last_message,
            }),
    )
    .with(Style::markdown())
    .to_string();

    output!(ctx, "{}", table);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::replica::tests::prepare_replication;
    use crate::context::tests::{bucket, bucket2, context, replica};
    use crate::context::CliContext;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_show_replica(
        context: CliContext,
        #[future] replica: String,
        #[future] bucket: String,
        #[future] bucket2: String,
    ) {
        let replica = replica.await;
        let bucket = bucket.await;
        let bucket2 = bucket2.await;

        prepare_replication(&context, &replica, &bucket, &bucket2)
            .await
            .unwrap();

        let args = show_replica_cmd()
            .get_matches_from(vec!["show", format!("local/{}", replica).as_str()]);
        build_client(&context, "local").await.unwrap();

        assert_eq!(show_replica_handler(&context, &args).await.unwrap(), ());
        let expected_info_table = build_info_table(vec![
            labeled_cell("Name", "test_replica"),
            labeled_cell("Active", true),
            labeled_cell("Mode", "Enabled"),
            labeled_cell("Provisioned", false),
            labeled_cell("Ok Records (hourly)", 0),
            labeled_cell("Errors (hourly)", 0),
            labeled_cell("Source Bucket", "test_bucket"),
            labeled_cell("Destination Bucket", "test_bucket_2"),
            labeled_cell("Destination Server", "http://localhost:8383"),
            labeled_cell("Entries", "[]"),
            "When: None".to_string(),
        ]);

        assert_eq!(
            context.stdout().history(),
            vec![
                expected_info_table,
                String::new(),
                "| Error Code | Count | Last Message |\n|------------|-------|--------------|"
                    .to_string()
            ]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_show_replica_invalid_path() {
        let args = show_replica_cmd().try_get_matches_from(vec!["show", "local"]);

        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'local' for '<REPLICATION_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }
}
