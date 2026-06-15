// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::lifecycle::helpers::format_mode_with_icon;
use crate::cmd::ALIAS_OR_URL_HELP;
use crate::io::std::output;
use clap::ArgAction::SetTrue;
use clap::{Arg, Command};
use reduct_rs::{LifecycleInfo, LifecycleType};
use tabled::settings::Style;
use tabled::{Table, Tabled};

pub(super) fn ls_lifecycle_cmd() -> Command {
    Command::new("ls")
        .about("List lifecycle policies")
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
                .help("Show detailed lifecycle information")
                .required(false),
        )
}

#[derive(Tabled)]
struct LifecycleTable {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Status")]
    status: String,
    #[tabled(rename = "Type")]
    lifecycle_type: String,
    #[tabled(rename = "Mode")]
    mode: String,
    #[tabled(rename = "Provisioned")]
    provisioned: String,
}

impl From<LifecycleInfo> for LifecycleTable {
    fn from(lifecycle: LifecycleInfo) -> Self {
        Self {
            name: lifecycle.name,
            status: if lifecycle.is_running {
                "✅ Running".to_string()
            } else {
                "⏸ Idle".to_string()
            },
            lifecycle_type: format_lifecycle_type(lifecycle.lifecycle_type),
            mode: format_mode_with_icon(lifecycle.mode),
            provisioned: if lifecycle.is_provisioned {
                "✓".to_string()
            } else {
                "-".to_string()
            },
        }
    }
}

fn format_lifecycle_type(lifecycle_type: LifecycleType) -> String {
    match lifecycle_type {
        LifecycleType::Delete => "Delete".to_string(),
        LifecycleType::Compress => "Compress".to_string(),
    }
}

pub(super) async fn ls_lifecycle(
    ctx: &crate::context::CliContext,
    args: &clap::ArgMatches,
) -> anyhow::Result<()> {
    let alias_or_url = args.get_one::<String>("ALIAS_OR_URL").unwrap();
    let client = crate::io::reduct::build_client(ctx, alias_or_url).await?;

    let print_list = |ctx: &crate::context::CliContext, lifecycle_list: Vec<LifecycleInfo>| {
        for lifecycle in lifecycle_list {
            output!(ctx, "{}", lifecycle.name);
        }
    };

    let print_full_list = |ctx: &crate::context::CliContext, lifecycle_list: Vec<LifecycleInfo>| {
        let table = Table::new(lifecycle_list.into_iter().map(LifecycleTable::from))
            .with(Style::markdown())
            .to_string();
        output!(ctx, "{}", table);
    };

    if args.get_flag("full") {
        print_full_list(ctx, client.list_lifecycles().await?);
    } else {
        print_list(ctx, client.list_lifecycles().await?);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::lifecycle::tests::{prepare_lifecycle, unique_name};
    use crate::context::tests::context;
    use crate::context::CliContext;
    use chrono::Utc;
    use reduct_rs::LifecycleMode;
    use rstest::rstest;

    #[test]
    fn test_lifecycle_table_includes_type() {
        let lifecycle = LifecycleInfo {
            name: "test-lifecycle".to_string(),
            is_provisioned: true,
            is_running: false,
            lifecycle_type: LifecycleType::Compress,
            mode: LifecycleMode::Enabled,
            last_run: Some(Utc::now()),
        };

        let row = LifecycleTable::from(lifecycle);
        assert_eq!(row.lifecycle_type, "Compress");
        assert_eq!(row.mode, "▶ Enabled");
        assert_eq!(row.provisioned, "✓");
    }

    #[rstest]
    #[tokio::test]
    async fn test_list_lifecycles(context: CliContext) {
        let lifecycle = unique_name("test-lifecycle");
        let bucket = unique_name("test-bucket");

        prepare_lifecycle(&context, &lifecycle, &bucket)
            .await
            .unwrap();

        let args = ls_lifecycle_cmd()
            .try_get_matches_from(vec!["ls", "local"])
            .unwrap();

        ls_lifecycle(&context, &args).await.unwrap();
        assert!(context.stdout().history().contains(&lifecycle));
    }

    #[rstest]
    #[tokio::test]
    async fn test_list_lifecycles_full(context: CliContext) {
        let lifecycle = unique_name("test-lifecycle");
        let bucket = unique_name("test-bucket");

        prepare_lifecycle(&context, &lifecycle, &bucket)
            .await
            .unwrap();

        let args = ls_lifecycle_cmd()
            .try_get_matches_from(vec!["ls", "local", "--full"])
            .unwrap();

        ls_lifecycle(&context, &args).await.unwrap();
        let output = context.stdout().history();
        assert_eq!(output.len(), 1);
        assert!(output[0].contains(&lifecycle));
        assert!(output[0].contains("Delete"));
        assert!(output[0].contains("▶ Enabled"));
    }
}
