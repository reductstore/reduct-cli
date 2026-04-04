// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::{Resource, ResourcePathParser};

use clap::{Arg, ArgMatches, Command};

pub(super) fn show_token_cmd() -> Command {
    Command::new("show")
        .about("Show token details")
        .arg(
            Arg::new("TOKEN_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg_required_else_help(true)
}

pub(super) async fn show_token(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, token_name) = args
        .get_one::<Resource>("TOKEN_PATH")
        .unwrap()
        .clone()
        .pair()?;

    let client = build_client(ctx, &alias_or_url).await?;
    let token = client.get_token_info(&token_name).await?;

    let bool_icon = |value: bool| if value { "✓" } else { "-" };

    output!(ctx, "Token: {}", token_name);
    output!(ctx, "Created: {}", token.created_at.date_naive());
    output!(ctx, "Provisioned: {}", bool_icon(token.is_provisioned));
    output!(ctx, "Expired: {}", bool_icon(token.is_expired));
    output!(
        ctx,
        "Expires At: {}",
        token
            .expires_at
            .map(|ts| ts.to_rfc3339())
            .unwrap_or("-".to_string())
    );
    output!(
        ctx,
        "TTL: {}",
        token
            .ttl
            .map(|ttl| ttl.to_string())
            .unwrap_or("-".to_string())
    );
    output!(
        ctx,
        "Last Access: {}",
        token
            .last_access
            .map(|ts| ts.to_rfc3339())
            .unwrap_or("-".to_string())
    );
    output!(ctx, "IP Allowlist: {:?}", token.ip_allowlist);

    let permissions = token.permissions.unwrap_or_default();
    output!(ctx, "Full Access: {}", bool_icon(permissions.full_access));
    output!(ctx, "Read Buckets: {:?}", permissions.read);
    output!(ctx, "Write Buckets: {:?}", permissions.write);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{context, token};
    use reduct_rs::Permissions;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_show_token(context: CliContext, #[future] token: String) {
        let token = token.await;
        let client = build_client(&context, "local").await.unwrap();
        client
            .create_token(&token, Permissions::default())
            .await
            .unwrap();

        let args =
            show_token_cmd().get_matches_from(vec!["show", format!("local/{}", token).as_str()]);
        show_token(&context, &args).await.unwrap();

        assert_eq!(context.stdout().history()[0], "Token: test_token");
        let history = context.stdout().history();
        assert!(history[1].starts_with("Created: "));
        let created = history[1].strip_prefix("Created: ").unwrap();
        assert_eq!(created.len(), 10);
        assert_eq!(context.stdout().history()[2], "Provisioned: -");
        assert_eq!(context.stdout().history()[3], "Expired: -");
        assert_eq!(context.stdout().history()[4], "Expires At: -");
        assert_eq!(context.stdout().history()[5], "TTL: -");
        assert_eq!(context.stdout().history()[6], "Last Access: -");
        assert_eq!(context.stdout().history()[7], "IP Allowlist: []");
        assert_eq!(context.stdout().history()[8], "Full Access: -");
        assert_eq!(context.stdout().history()[9], "Read Buckets: []");
        assert_eq!(context.stdout().history()[10], "Write Buckets: []");
    }

    #[rstest]
    #[tokio::test]
    async fn test_show_token_bad_path() {
        let cmd = show_token_cmd();
        let args = cmd.try_get_matches_from(vec!["show", "test"]);
        assert_eq!(args.unwrap_err().to_string(), "error: invalid value 'test' for '<TOKEN_PATH>'\n\nFor more information, try '--help'.\n");
    }
}
