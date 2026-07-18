use clap::{Arg, Command};
use regex::Regex;

use crate::{context::CliContext, io::reduct::build_client};

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

pub(crate) async fn write_handler(ctx: &CliContext, args: &clap::ArgMatches) -> anyhow::Result<()> {
    println!("write_handler");

    let entry_path = args.get_one::<String>("ENTRY_PATH").unwrap().clone();
    let (alias_or_url, bucket_name, entry_name) = parse_entry_path(&entry_path)?;
    let payload = args.get_one::<String>("payload").unwrap().clone();

    let client = build_client(ctx, &alias_or_url).await?;

    Ok(())
}

fn parse_entry_path(entry_path: &str) -> anyhow::Result<(String, String, String)> {
    let re = Regex::new(r"^(?P<alias_or_url>[^/]+)/(?P<bucket_name>[^/]+)/(?P<entry_name>.+)$")?;
    let caps = re.captures(entry_path).ok_or_else(|| {
        anyhow::anyhow!(
            "Invalid entry path. Expected ALIAS/BUCKET/ENTRY, got {}",
            entry_path
        )
    })?;

    let alias_or_url = caps.name("alias_or_url").unwrap().as_str().to_string();
    let bucket_name = caps.name("bucket_name").unwrap().as_str().to_string();
    let entry_name = caps.name("entry_name").unwrap().as_str().to_string();

    Ok((alias_or_url, bucket_name, entry_name))
}
