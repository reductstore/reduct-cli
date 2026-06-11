// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::lifecycle::helpers::format_mode_with_icon;
use crate::cmd::table::{build_info_table, labeled_cell};
use crate::cmd::RESOURCE_PATH_HELP;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::Resource;
use clap::{Arg, Command};

pub(super) fn show_lifecycle_cmd() -> Command {
    Command::new("show")
        .about("Show details about a lifecycle policy")
        .arg(
            Arg::new("LIFECYCLE_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(crate::parse::ResourcePathParser::new())
                .required(true),
        )
}

pub(super) async fn show_lifecycle_handler(
    ctx: &crate::context::CliContext,
    args: &clap::ArgMatches,
) -> anyhow::Result<()> {
    let (alias_or_url, lifecycle_name) = args
        .get_one::<Resource>("LIFECYCLE_PATH")
        .unwrap()
        .clone()
        .pair()?;
    let client = build_client(ctx, &alias_or_url).await?;

    let lifecycle = client.get_lifecycle(&lifecycle_name).await?;

    let mut info_cells = vec![
        labeled_cell("Name", lifecycle.info.name.clone()),
        labeled_cell(
            "Status",
            if lifecycle.info.is_running {
                "✅ Running"
            } else {
                "⏸ Idle"
            },
        ),
        labeled_cell("Mode", format_mode_with_icon(lifecycle.info.mode)),
        labeled_cell(
            "Provisioned",
            if lifecycle.info.is_provisioned {
                "✓"
            } else {
                "-"
            },
        ),
        labeled_cell("Type", format!("{:?}", lifecycle.settings.lifecycle_type)),
        labeled_cell("Bucket", lifecycle.settings.bucket.clone()),
        labeled_cell("Older Than", lifecycle.settings.older_than.clone()),
        labeled_cell("Interval", lifecycle.settings.interval.clone()),
        labeled_cell("Entries", format!("{:?}", lifecycle.settings.entries)),
    ];

    let when_value = lifecycle
        .settings
        .when
        .as_ref()
        .map(|value| serde_json::to_string_pretty(value))
        .transpose()?;
    let when_cell_value = when_value
        .map(|value| value.replace('\n', "\n  "))
        .unwrap_or_else(|| "None".to_string());
    info_cells.push(labeled_cell("When", when_cell_value));

    let info_table = build_info_table(info_cells);
    output!(ctx, "{}", info_table);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::lifecycle::tests::{prepare_lifecycle, unique_name};
    use crate::context::tests::context;
    use crate::context::CliContext;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_show_lifecycle(context: CliContext) {
        let lifecycle = unique_name("test-lifecycle");
        let bucket = unique_name("test-bucket");

        prepare_lifecycle(&context, &lifecycle, &bucket)
            .await
            .unwrap();

        let args = show_lifecycle_cmd()
            .get_matches_from(vec!["show", format!("local/{}", lifecycle).as_str()]);

        assert_eq!(show_lifecycle_handler(&context, &args).await.unwrap(), ());
        let output = context.stdout().history();
        assert_eq!(output.len(), 1);
        assert!(output[0].contains(&lifecycle));
        assert!(output[0].contains(&bucket));
        assert!(output[0].contains("Mode: ▶ Enabled"));
        assert!(output[0].contains("Type: Delete"));
        assert!(output[0].contains("Older Than: 1h"));
        assert!(output[0].contains("Interval: 10m"));
        assert!(output[0].contains("When: None"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_show_lifecycle_invalid_path() {
        let args = show_lifecycle_cmd().try_get_matches_from(vec!["show", "local"]);

        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'local' for '<LIFECYCLE_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }
}
