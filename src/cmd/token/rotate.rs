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

pub(super) fn rotate_token_cmd() -> Command {
    Command::new("rotate")
        .about("Rotate an access token value")
        .arg(
            Arg::new("TOKEN_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg_required_else_help(true)
}

pub(super) async fn rotate_token(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, token_name) = args
        .get_one::<Resource>("TOKEN_PATH")
        .unwrap()
        .clone()
        .pair()?;

    let client = build_client(ctx, &alias_or_url).await?;
    let token = client.rotate_token(&token_name).await?;
    output!(ctx, "Token '{}' rotated: {}", token_name, token.value);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{context, token};
    use reduct_rs::{ErrorCode, Permissions};
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_rotate_token(context: CliContext, #[future] token: String) {
        let token = token.await;
        let client = build_client(&context, "local").await.unwrap();
        client
            .create_token(&token, Permissions::default())
            .await
            .unwrap();

        let args = rotate_token_cmd()
            .get_matches_from(vec!["rotate", format!("local/{}", token).as_str()]);
        if let Err(err) = rotate_token(&context, &args).await {
            if let Some(err) = err.downcast_ref::<reduct_rs::ReductError>() {
                if err.status() == ErrorCode::MethodNotAllowed {
                    return;
                }
            }
            panic!("unexpected error: {err}");
        }

        assert!(
            context.stdout().history()[0].starts_with("Token 'test_token' rotated: test_token-")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rotate_token_bad_path() {
        let cmd = rotate_token_cmd();
        let args = cmd.try_get_matches_from(vec!["rotate", "test"]);
        assert_eq!(args.unwrap_err().to_string(), "error: invalid value 'test' for '<TOKEN_PATH>'\n\nFor more information, try '--help'.\n");
    }
}
