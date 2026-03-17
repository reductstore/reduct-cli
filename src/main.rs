// Copyright 2023-2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod cmd;
mod config;
mod context;
mod helpers;
mod io;
mod parse;

use crate::cmd::alias::{alias_cmd, alias_handler};
use crate::cmd::attachment::{attachment_cmd, attachment_handler};
use crate::context::ContextBuilder;
use std::time::Duration;

use crate::cmd::bucket::{bucket_cmd, bucket_handler};
use crate::cmd::cp::{cp_cmd, cp_handler};
use crate::cmd::replica::{replication_cmd, replication_handler};
use crate::cmd::rm::{rm_cmd, rm_handler};
use crate::cmd::server::{server_cmd, server_handler};
use crate::cmd::token::{token_cmd, token_handler};
use clap::ArgAction::SetTrue;
use clap::{crate_description, crate_name, crate_version, value_parser, Arg, Command};
use colored::Colorize;

fn cli() -> Command {
    Command::new(crate_name!())
        .version(crate_version!())
        .arg_required_else_help(true)
        .about(crate_description!())
        .arg(
            Arg::new("ignore-ssl")
                .long("ignore-ssl")
                .short('i')
                .help("Ignore SSL certificate verification")
                .required(false)
                .action(SetTrue)
                .global(true),
        )
        .arg(
            Arg::new("ca-cert")
                .long("ca-cert")
                .value_name("PATH")
                .help("Path to a custom CA certificate bundle/file")
                .required(false)
                .global(true),
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
                .short('T')
                .value_name("SECONDS")
                .help("Timeout for requests")
                .value_parser(value_parser!(u64))
                .required(false)
                .global(true),
        )
        .arg(
            Arg::new("parallel")
                .long("parallel")
                .short('p')
                .value_name("COUNT")
                .help("Number of parallel requests")
                .value_parser(value_parser!(usize))
                .required(false)
                .global(true),
        )
        .subcommand(alias_cmd())
        .subcommand(attachment_cmd())
        .subcommand(server_cmd())
        .subcommand(bucket_cmd())
        .subcommand(token_cmd())
        .subcommand(replication_cmd())
        .subcommand(cp_cmd())
        .subcommand(rm_cmd())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let matches = cli().get_matches();
    let ignore_ssl = matches.get_flag("ignore-ssl").then_some(true);
    let timeout = matches.get_one::<u64>("timeout").copied();
    let parallel = matches.get_one::<usize>("parallel").copied();
    let ca_cert = matches.get_one::<String>("ca-cert").cloned();

    let ctx = ContextBuilder::new()
        .ignore_ssl(ignore_ssl)
        .timeout(timeout.map(Duration::from_secs))
        .parallel(parallel)
        .ca_cert(ca_cert)
        .build();

    let result = match matches.subcommand() {
        Some(("alias", args)) => alias_handler(&ctx, args.subcommand()),
        Some(("attachment", args)) => attachment_handler(&ctx, args.subcommand()).await,
        Some(("server", args)) => server_handler(&ctx, args.subcommand()).await,
        Some(("bucket", args)) => bucket_handler(&ctx, args.subcommand()).await,
        Some(("token", args)) => token_handler(&ctx, args.subcommand()).await,
        Some(("replica", args)) => replication_handler(&ctx, args.subcommand()).await,

        Some(("cp", args)) => cp_handler(&ctx, args).await,
        Some(("rm", args)) => rm_handler(&ctx, args).await,
        _ => Ok(()),
    };

    if let Err(err) = result {
        eprintln!("{}", err.to_string().red().bold(),);
        std::process::exit(1);
    }

    Ok(())
}
