// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::{Resource, ResourcePathParser};
use chrono::SecondsFormat;

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
    let is_json = ctx.json();
    let client = build_client(ctx, &alias_or_url).await?;
    let token = client.get_token(&token_name).await?;

    let bool_icon = |value: bool| if value { "✓" } else { "-" };

    if is_json {
        let permissions = token.permissions.unwrap_or_default();
        let token_json = serde_json::json!({
            "token": token_name,
            "created": token.created_at,
            "provisioned": token.is_provisioned,
            "status": token.is_expired,
            "expires_at": token.expires_at,
            "ttl": token.ttl,
            "last_access": token.last_access,
            "full_access": permissions.full_access,
            "read_access": permissions.read,
            "write_access": permissions.write,
        });

        output!(ctx, "{}", serde_json::to_string(&token_json).unwrap());
        return Ok(());
    }
    output!(ctx, "Token: {}", token_name);
    output!(ctx, "Created: {}", token.created_at.date_naive());
    output!(ctx, "Provisioned: {}", bool_icon(token.is_provisioned));
    output!(
        ctx,
        "Status: {}",
        if token.is_expired {
            "❌ Expired"
        } else {
            "✅ Valid"
        }
    );
    output!(
        ctx,
        "Expires At: {}",
        token
            .expires_at
            .map(|ts| ts.to_rfc3339_opts(SecondsFormat::Secs, true))
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
            .map(|ts| ts.to_rfc3339_opts(SecondsFormat::Secs, true))
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
    use crate::context::{
        tests::{context, token, MockOutput},
        ContextBuilder,
    };
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
        assert_eq!(context.stdout().history()[3], "Status: ✅ Valid");
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

    #[rstest]
    #[tokio::test]
    async fn test_show_token_json(context: CliContext, #[future] token: String) {
        let token = token.await;

        let ctx = ContextBuilder::new()
            .config_path(context.config_path())
            .json(Some(true))
            .output(Box::new(MockOutput::new()))
            .build();

        let client = build_client(&ctx, "local").await.unwrap();
        client
            .create_token(&token, Permissions::default())
            .await
            .unwrap();

        let args =
            show_token_cmd().get_matches_from(vec!["show", format!("local/{}", token).as_str()]);

        show_token(&ctx, &args).await.unwrap();
        let json: serde_json::Value = serde_json::from_str(&ctx.stdout().history()[0]).unwrap();

        assert_eq!(json["token"], "test_token");
        assert!(json["created"].as_str().unwrap().len() > 1);
        assert_eq!(json["provisioned"], false);
        assert_eq!(json["status"], false);
        assert_eq!(json["expires_at"], serde_json::Value::Null);
        assert_eq!(json["ttl"], serde_json::Value::Null);
        assert_eq!(json["last_access"], serde_json::Value::Null);
        assert_eq!(json["read_access"], serde_json::json!([]));
        assert_eq!(json["write_access"], serde_json::json!([]));
    }
}
