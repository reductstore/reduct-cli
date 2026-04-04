// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::ALIAS_OR_URL_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::TokenInfo;
use tabled::settings::Style;
use tabled::{Table, Tabled};

pub(super) fn ls_tokens_cmd() -> Command {
    Command::new("ls")
        .about("List access tokens")
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
                .help("Show detailed token information")
                .required(false),
        )
}

pub(super) async fn ls_tokens(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let alias_or_url = args.get_one::<String>("ALIAS_OR_URL").unwrap();
    let client = build_client(ctx, alias_or_url).await?;

    let token_list = client.list_tokens_info().await?;
    if args.get_flag("full") {
        print_full_list(ctx, token_list);
    } else {
        print_list(ctx, token_list);
    }

    Ok(())
}

fn print_list(ctx: &CliContext, token_list: Vec<TokenInfo>) {
    for token in token_list {
        output!(ctx, "{}", token.name);
    }
}

#[derive(Tabled)]
struct TokenTable {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Provisioned")]
    provisioned: String,
    #[tabled(rename = "Expired")]
    expired: String,
    #[tabled(rename = "Expires At (UTC)")]
    expires_at: String,
    #[tabled(rename = "TTL (s)")]
    ttl: String,
    #[tabled(rename = "Last Access (UTC)")]
    last_access: String,
    #[tabled(rename = "IP Allowlist")]
    ip_allowlist: String,
}

impl From<TokenInfo> for TokenTable {
    fn from(token: TokenInfo) -> Self {
        Self {
            name: token.name,
            provisioned: if token.is_provisioned {
                "✓".to_string()
            } else {
                "-".to_string()
            },
            expired: if token.is_expired {
                "✓".to_string()
            } else {
                "-".to_string()
            },
            expires_at: token
                .expires_at
                .map(|ts| ts.to_rfc3339())
                .unwrap_or("-".to_string()),
            ttl: token
                .ttl
                .map(|ttl| ttl.to_string())
                .unwrap_or("-".to_string()),
            last_access: token
                .last_access
                .map(|ts| ts.to_rfc3339())
                .unwrap_or("-".to_string()),
            ip_allowlist: if token.ip_allowlist.is_empty() {
                "-".to_string()
            } else {
                token.ip_allowlist.join(", ")
            },
        }
    }
}

fn print_full_list(ctx: &CliContext, token_list: Vec<TokenInfo>) {
    if token_list.is_empty() {
        return;
    }

    let table = Table::new(token_list.into_iter().map(TokenTable::from))
        .with(Style::markdown())
        .to_string();
    output!(ctx, "{}", table);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{context, token};
    use reduct_rs::Permissions;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_ls_tokens(context: CliContext, #[future] token: String) {
        let token = token.await;
        let client = build_client(&context, "local").await.unwrap();
        client
            .create_token(&token, Permissions::default())
            .await
            .unwrap();

        let args = ls_tokens_cmd().get_matches_from(vec!["ls", "local"]);
        ls_tokens(&context, &args).await.unwrap();

        assert_eq!(
            context.stdout().history(),
            vec!["init-token", token.as_str()]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_ls_tokens_full(context: CliContext, #[future] token: String) {
        let token = token.await;
        let client = build_client(&context, "local").await.unwrap();
        client
            .create_token(&token, Permissions::default())
            .await
            .unwrap();
        let token_list = client.list_tokens_info().await.unwrap();

        let args = ls_tokens_cmd().get_matches_from(vec!["ls", "local", "--full"]);
        ls_tokens(&context, &args).await.unwrap();

        let expected = Table::new(token_list.into_iter().map(TokenTable::from))
            .with(Style::markdown())
            .to_string();
        assert_eq!(context.stdout().history(), vec![expected]);
    }
}
