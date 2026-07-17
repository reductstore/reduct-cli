use clap::{Arg, Command};

use crate::context::CliContext;

pub(crate) fn write_cmd() -> Command {
    Command::new("write")
        .about("Write single record to a bucket")
        .arg(
            Arg::new("ENTRY_PATH")
                .help("Full path the entry to write to")
                .required(true),
        )
        .arg(
            Arg::new("payload")
                .long("string")
                .short('s')
                .help("inline payload string.")
                .required(true)
                .value_name("PAYLOAD")
                .conflicts_with("path"),
        )
}

pub(crate) async fn write_handler(
    _ctx: &CliContext,
    args: &clap::ArgMatches,
) -> anyhow::Result<()> {
    println!("write_handler");

    let entry_path = args.get_one::<String>("ENTRY_PATH").unwrap().clone();
    let payload = args.get_one::<String>("payload").unwrap().clone();

    println!("{:?}", entry_path);
    println!("{:?}", payload);

    Ok(())
}
