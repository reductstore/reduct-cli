// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::ResourcePathParser;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::ReplicationMode;

fn replication_mode_cmd(name: &'static str, about: &'static str) -> Command {
    Command::new(name).about(about).arg(
        Arg::new("REPLICATION_PATH")
            .help(RESOURCE_PATH_HELP)
            .value_parser(ResourcePathParser::new())
            .required(true),
    )
}

pub(super) fn enable_replica_cmd() -> Command {
    replication_mode_cmd("enable", "Enable a replication task")
}

pub(super) fn disable_replica_cmd() -> Command {
    replication_mode_cmd("disable", "Disable a replication task")
}

pub(super) fn pause_replica_cmd() -> Command {
    replication_mode_cmd("pause", "Pause a replication task")
}

async fn set_replica_mode(
    ctx: &CliContext,
    args: &ArgMatches,
    mode: ReplicationMode,
    action: &str,
) -> anyhow::Result<()> {
    let (alias_or_url, replication_name) = args
        .get_one::<(String, String)>("REPLICATION_PATH")
        .unwrap();

    let client = build_client(ctx, alias_or_url).await?;
    client.set_replication_mode(replication_name, mode).await?;

    output!(ctx, "Replication '{}' {}", replication_name, action);
    Ok(())
}

pub(super) async fn enable_replica_handler(
    ctx: &CliContext,
    args: &ArgMatches,
) -> anyhow::Result<()> {
    set_replica_mode(ctx, args, ReplicationMode::Enabled, "enabled").await
}

pub(super) async fn disable_replica_handler(
    ctx: &CliContext,
    args: &ArgMatches,
) -> anyhow::Result<()> {
    set_replica_mode(ctx, args, ReplicationMode::Disabled, "disabled").await
}

pub(super) async fn pause_replica_handler(
    ctx: &CliContext,
    args: &ArgMatches,
) -> anyhow::Result<()> {
    set_replica_mode(ctx, args, ReplicationMode::Paused, "paused").await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::replica::tests::prepare_replication;
    use crate::context::tests::{bucket, bucket2, context, replica};
    use reduct_rs::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    #[case::enable("enable", ReplicationMode::Enabled, "enabled")]
    #[case::disable("disable", ReplicationMode::Disabled, "disabled")]
    #[case::pause("pause", ReplicationMode::Paused, "paused")]
    async fn test_set_replica_mode(
        context: CliContext,
        #[future] replica: String,
        #[future] bucket: String,
        #[future] bucket2: String,
        #[case] subcommand: &str,
        #[case] mode: ReplicationMode,
        #[case] action: &str,
    ) {
        let test_replica = replica.await;
        let bucket = bucket.await;
        let bucket2 = bucket2.await;

        let client = prepare_replication(&context, &test_replica, &bucket, &bucket2)
            .await
            .unwrap();

        if let Err(err) = client.set_replication_mode(&test_replica, mode).await {
            if err.status() == ErrorCode::MethodNotAllowed {
                eprintln!("Server does not support replication mode endpoint yet.");
                return;
            }
            panic!("{err:?}");
        }

        match subcommand {
            "enable" => {
                let args = enable_replica_cmd()
                    .try_get_matches_from(vec![
                        subcommand,
                        format!("local/{}", test_replica).as_str(),
                    ])
                    .unwrap();
                enable_replica_handler(&context, &args).await.unwrap();
            }
            "disable" => {
                let args = disable_replica_cmd()
                    .try_get_matches_from(vec![
                        subcommand,
                        format!("local/{}", test_replica).as_str(),
                    ])
                    .unwrap();
                disable_replica_handler(&context, &args).await.unwrap();
            }
            "pause" => {
                let args = pause_replica_cmd()
                    .try_get_matches_from(vec![
                        subcommand,
                        format!("local/{}", test_replica).as_str(),
                    ])
                    .unwrap();
                pause_replica_handler(&context, &args).await.unwrap();
            }
            _ => unreachable!(),
        }

        let replica = client.get_replication(&test_replica).await.unwrap();
        assert_eq!(replica.info.mode, mode);
        assert_eq!(replica.settings.mode, mode);
        assert_eq!(
            context.stdout().history(),
            vec![format!("Replication '{}' {}", test_replica, action)]
        );
    }
}
