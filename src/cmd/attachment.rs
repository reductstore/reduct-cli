// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod helpers;
mod ls;
mod read;
mod rm;
mod write;

use crate::context::CliContext;
use clap::{ArgMatches, Command};

pub(crate) fn attachment_cmd() -> Command {
    Command::new("attachment")
        .about("Manage entry attachments")
        .arg_required_else_help(true)
        .subcommand(ls::ls_attachment_cmd())
        .subcommand(read::read_attachment_cmd())
        .subcommand(rm::rm_attachment_cmd())
        .subcommand(write::write_attachment_cmd())
}

pub(crate) async fn attachment_handler(
    ctx: &CliContext,
    matches: Option<(&str, &ArgMatches)>,
) -> anyhow::Result<()> {
    match matches {
        Some(("ls", args)) => ls::ls_attachment(ctx, args).await?,
        Some(("read", args)) => read::read_attachment(ctx, args).await?,
        Some(("rm", args)) => rm::rm_attachment(ctx, args).await?,
        Some(("write", args)) => write::write_attachment(ctx, args).await?,
        _ => (),
    }

    Ok(())
}
