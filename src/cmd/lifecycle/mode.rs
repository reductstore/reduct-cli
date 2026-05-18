// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::{Resource, ResourcePathParser};
use clap::{Arg, ArgMatches, Command};
use reduct_rs::LifecycleMode;

fn lifecycle_mode_cmd(name: &'static str, about: &'static str) -> Command {
    Command::new(name).about(about).arg(
        Arg::new("LIFECYCLE_PATH")
            .help(RESOURCE_PATH_HELP)
            .value_parser(ResourcePathParser::new())
            .required(true),
    )
}

pub(super) fn enable_lifecycle_cmd() -> Command {
    lifecycle_mode_cmd("enable", "Enable a lifecycle policy")
}

pub(super) fn disable_lifecycle_cmd() -> Command {
    lifecycle_mode_cmd("disable", "Disable a lifecycle policy")
}

pub(super) fn dry_run_lifecycle_cmd() -> Command {
    lifecycle_mode_cmd("dry-run", "Set lifecycle policy mode to dry run")
}

async fn set_lifecycle_mode(
    ctx: &CliContext,
    args: &ArgMatches,
    mode: LifecycleMode,
    action: &str,
) -> anyhow::Result<()> {
    let (alias_or_url, lifecycle_name) = args
        .get_one::<Resource>("LIFECYCLE_PATH")
        .unwrap()
        .clone()
        .pair()?;

    let client = build_client(ctx, &alias_or_url).await?;
    client.set_lifecycle_mode(&lifecycle_name, mode).await?;

    output!(ctx, "Lifecycle '{}' {}", lifecycle_name, action);
    Ok(())
}

pub(super) async fn enable_lifecycle_handler(
    ctx: &CliContext,
    args: &ArgMatches,
) -> anyhow::Result<()> {
    set_lifecycle_mode(ctx, args, LifecycleMode::Enabled, "enabled").await
}

pub(super) async fn disable_lifecycle_handler(
    ctx: &CliContext,
    args: &ArgMatches,
) -> anyhow::Result<()> {
    set_lifecycle_mode(ctx, args, LifecycleMode::Disabled, "disabled").await
}

pub(super) async fn dry_run_lifecycle_handler(
    ctx: &CliContext,
    args: &ArgMatches,
) -> anyhow::Result<()> {
    set_lifecycle_mode(ctx, args, LifecycleMode::DryRun, "set to dry run").await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::lifecycle::tests::{prepare_lifecycle, unique_name};
    use crate::context::tests::context;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    #[case::enable("enable", LifecycleMode::Enabled, "enabled")]
    #[case::disable("disable", LifecycleMode::Disabled, "disabled")]
    #[case::dry_run("dry-run", LifecycleMode::DryRun, "set to dry run")]
    async fn test_set_lifecycle_mode(
        context: CliContext,
        #[case] subcommand: &str,
        #[case] mode: LifecycleMode,
        #[case] action: &str,
    ) {
        let test_lifecycle = unique_name("test-lifecycle");
        let bucket = unique_name("test-bucket");

        let client = prepare_lifecycle(&context, &test_lifecycle, &bucket)
            .await
            .unwrap();

        match subcommand {
            "enable" => {
                let args = enable_lifecycle_cmd()
                    .try_get_matches_from(vec![
                        subcommand,
                        format!("local/{}", test_lifecycle).as_str(),
                    ])
                    .unwrap();
                enable_lifecycle_handler(&context, &args).await.unwrap();
            }
            "disable" => {
                let args = disable_lifecycle_cmd()
                    .try_get_matches_from(vec![
                        subcommand,
                        format!("local/{}", test_lifecycle).as_str(),
                    ])
                    .unwrap();
                disable_lifecycle_handler(&context, &args).await.unwrap();
            }
            "dry-run" => {
                let args = dry_run_lifecycle_cmd()
                    .try_get_matches_from(vec![
                        subcommand,
                        format!("local/{}", test_lifecycle).as_str(),
                    ])
                    .unwrap();
                dry_run_lifecycle_handler(&context, &args).await.unwrap();
            }
            _ => unreachable!(),
        }

        let lifecycle = client.get_lifecycle(&test_lifecycle).await.unwrap();
        assert_eq!(lifecycle.info.mode, mode);
        assert_eq!(lifecycle.settings.mode, mode);
        assert_eq!(
            context.stdout().history(),
            vec![format!("Lifecycle '{}' {}", test_lifecycle, action)]
        );
    }
}
