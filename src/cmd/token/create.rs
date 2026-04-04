// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::{Resource, ResourcePathParser};
use chrono::{DateTime, Utc};
use clap::ArgAction::{Append, SetTrue};
use clap::{Arg, ArgMatches, Command};
use reduct_rs::{Permissions, ReductClient, TokenCreateOptions};

pub(super) fn create_token_cmd() -> Command {
    Command::new("create")
        .about("Create an access token")
        .arg(
            Arg::new("TOKEN_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("full-access")
                .long("full-access")
                .short('A')
                .action(SetTrue)
                .help("Give full access to the token")
                .required(false),
        )
        .arg(
            Arg::new("read-bucket")
                .long("read-bucket")
                .short('r')
                .value_name("TEST")
                .num_args(1..)
                .help("Bucket to give read access to. Can be used multiple times")
                .required(false),
        )
        .arg(
            Arg::new("write-bucket")
                .long("write-bucket")
                .short('w')
                .value_name("TEST")
                .num_args(1..)
                .help("Bucket to give write access to. Can be used multiple times")
                .required(false),
        )
        .arg(
            Arg::new("ttl")
                .long("ttl")
                .value_name("SECONDS")
                .value_parser(clap::value_parser!(u64))
                .help("Time to live in seconds")
                .required(false)
                .conflicts_with("expires-at"),
        )
        .arg(
            Arg::new("expires-at")
                .long("expires-at")
                .value_name("RFC3339")
                .help("Expiration date in RFC3339 format (e.g. 2026-04-05T12:00:00Z)")
                .required(false)
                .conflicts_with("ttl"),
        )
        .arg(
            Arg::new("ip-allow")
                .long("ip-allow")
                .value_name("IP_OR_CIDR")
                .action(Append)
                .num_args(1..)
                .help("Allowed source IP or CIDR. Can be used multiple times")
                .required(false),
        )
}

pub(super) async fn create_token(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, token_name) = args
        .get_one::<Resource>("TOKEN_PATH")
        .unwrap()
        .clone()
        .pair()?;
    let full_access = args.get_flag("full-access");
    let read_buckets = args
        .get_many::<String>("read-bucket")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    let write_buckets = args
        .get_many::<String>("write-bucket")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    let ttl = args.get_one::<u64>("ttl").copied();
    let expires_at = args
        .get_one::<String>("expires-at")
        .map(|expires_at| {
            DateTime::parse_from_rfc3339(expires_at)
                .map(|ts| ts.with_timezone(&Utc))
                .map_err(|e| anyhow::anyhow!("invalid --expires-at '{}': {}", expires_at, e))
        })
        .transpose()?;
    let ip_allowlist = args
        .get_many::<String>("ip-allow")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let client: ReductClient = build_client(ctx, &alias_or_url).await?;
    let token = client
        .create_token_with_options(
            &token_name,
            TokenCreateOptions {
                permissions: Permissions {
                    full_access,
                    read: read_buckets,
                    write: write_buckets,
                },
                expires_at,
                ttl,
                ip_allowlist,
            },
        )
        .await?;

    output!(ctx, "Token '{}' created: {}", token_name, token.value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{context, token};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_create_token(context: CliContext, #[future] token: String) {
        let args = create_token_cmd()
            .try_get_matches_from(vec![
                "create",
                format!("local/{}", token.await).as_str(),
                "--full-access",
                "--ttl",
                "60",
                "--ip-allow",
                "127.0.0.1",
                "--read-bucket",
                "test",
                "--write-bucket",
                "test",
            ])
            .unwrap();
        create_token(&context, &args).await.unwrap();
        assert!(
            context.stdout().history()[0].starts_with("Token 'test_token' created: test_token-")
        );
    }

    #[rstest]
    fn test_create_token_bad_path() {
        let cmd = create_token_cmd();
        let args = cmd.try_get_matches_from(vec!["create", "test"]);
        assert_eq!(args.unwrap_err().to_string(), "error: invalid value 'test' for '<TOKEN_PATH>'\n\nFor more information, try '--help'.\n");
    }

    #[rstest]
    fn test_create_token_ttl_expires_conflict() {
        let cmd = create_token_cmd();
        let err = cmd
            .try_get_matches_from(vec![
                "create",
                "local/test_token",
                "--ttl",
                "60",
                "--expires-at",
                "2026-04-05T12:00:00Z",
            ])
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("cannot be used with '--expires-at <RFC3339>'"));
    }
}
