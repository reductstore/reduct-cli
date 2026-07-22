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
use crate::cmd::write::{write_handler, write_record_cmd};
use crate::context::ContextBuilder;
use serde_json::json;
use std::time::Duration;

use crate::cmd::bucket::{bucket_cmd, bucket_handler};
use crate::cmd::cp::{cp_cmd, cp_handler};
use crate::cmd::lifecycle::{lifecycle_cmd, lifecycle_handler};
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
        .arg(
            Arg::new("json")
                .long("json")
                .short('j')
                .help("Print output in JSON format")
                .required(false)
                .action(SetTrue)
                .global(true),
        )
        .subcommand(alias_cmd())
        .subcommand(attachment_cmd())
        .subcommand(server_cmd())
        .subcommand(bucket_cmd())
        .subcommand(token_cmd())
        .subcommand(replication_cmd())
        .subcommand(lifecycle_cmd())
        .subcommand(cp_cmd())
        .subcommand(rm_cmd())
        .subcommand(write_record_cmd())
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
        Some(("lifecycle", args)) => lifecycle_handler(&ctx, args.subcommand()).await,

        Some(("cp", args)) => cp_handler(&ctx, args).await,
        Some(("rm", args)) => rm_handler(&ctx, args).await,
        Some(("write", args)) => write_handler(&ctx, args).await,
        _ => Ok(()),
    };

    let command = matches.subcommand().unwrap().0;

    if let Err(err) = result {
        // Do not output json if the command is "cp"
        if matches.get_flag("json") && command != "cp" {
            let json_error = print_json_error(&err);
            eprintln!("{}", serde_json::to_string_pretty(&json_error)?);
            std::process::exit(1);
        }

        eprintln!("{}", err.to_string().red().bold(),);
        std::process::exit(1);
    }

    Ok(())
}

fn print_json_error(err: &anyhow::Error) -> serde_json::Value {
    // Try to downcast to ReductError to get the status code
    let (status_code, error_message) =
        if let Some(reduct_err) = err.downcast_ref::<reduct_rs::ReductError>() {
            (reduct_err.status() as i32, reduct_err.message().to_string())
        } else {
            // If not a ReductError, use 1 as unknown status
            (1, err.to_string())
        };

    json!({
        "status_code": status_code,
        "error_message": error_message,
    })
}
