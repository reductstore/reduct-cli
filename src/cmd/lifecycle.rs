// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod create;
mod helpers;
mod ls;
mod mode;
mod rm;
mod show;
mod update;

use crate::cmd::lifecycle::ls::{ls_lifecycle, ls_lifecycle_cmd};
use crate::cmd::lifecycle::mode::{
    disable_lifecycle_cmd, disable_lifecycle_handler, dry_run_lifecycle_cmd,
    dry_run_lifecycle_handler, enable_lifecycle_cmd, enable_lifecycle_handler,
};
use crate::cmd::lifecycle::rm::{rm_lifecycle_cmd, rm_lifecycle_handler};
use crate::cmd::lifecycle::show::{show_lifecycle_cmd, show_lifecycle_handler};
use crate::cmd::lifecycle::update::{update_lifecycle_cmd, update_lifecycle_handler};
use clap::Command;
use create::{create_lifecycle, create_lifecycle_cmd};

pub(crate) fn lifecycle_cmd() -> Command {
    Command::new("lifecycle")
        .visible_alias("lc")
        .about("Manage lifecycle policies")
        .arg_required_else_help(true)
        .subcommand(create_lifecycle_cmd())
        .subcommand(update_lifecycle_cmd())
        .subcommand(ls_lifecycle_cmd())
        .subcommand(show_lifecycle_cmd())
        .subcommand(enable_lifecycle_cmd())
        .subcommand(disable_lifecycle_cmd())
        .subcommand(dry_run_lifecycle_cmd())
        .subcommand(rm_lifecycle_cmd())
}

pub(crate) async fn lifecycle_handler(
    ctx: &crate::context::CliContext,
    matches: Option<(&str, &clap::ArgMatches)>,
) -> anyhow::Result<()> {
    match matches {
        Some(("create", args)) => create_lifecycle(ctx, args).await?,
        Some(("update", args)) => update_lifecycle_handler(ctx, args).await?,
        Some(("ls", args)) => ls_lifecycle(ctx, args).await?,
        Some(("show", args)) => show_lifecycle_handler(ctx, args).await?,
        Some(("enable", args)) => enable_lifecycle_handler(ctx, args).await?,
        Some(("disable", args)) => disable_lifecycle_handler(ctx, args).await?,
        Some(("dry-run", args)) => dry_run_lifecycle_handler(ctx, args).await?,
        Some(("rm", args)) => rm_lifecycle_handler(ctx, args).await?,
        _ => (),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::context::CliContext;
    use crate::io::reduct::build_client;
    use reduct_rs::ReductClient;
    use std::time::{SystemTime, UNIX_EPOCH};

    pub(crate) fn unique_name(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{}-{}", prefix, nanos)
    }

    pub(crate) async fn prepare_lifecycle(
        context: &CliContext,
        lifecycle: &str,
        bucket: &str,
    ) -> anyhow::Result<ReductClient> {
        let client = build_client(context, "local").await?;
        client.create_bucket(bucket).send().await?;

        client
            .create_lifecycle(lifecycle)
            .bucket(bucket)
            .older_than("1h")
            .interval("10m")
            .send()
            .await?;

        Ok(client)
    }
}
