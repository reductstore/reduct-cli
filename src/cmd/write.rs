use clap::{Arg, Command};

pub(crate) fn write_cmd() -> Command {
    Command::new("write")
        .about("Write single record to a bucket")
        .arg(
            Arg::new("entry-path")
                .help("Full path the entry to write to")
                .required(true),
        )
        .arg(
            Arg::new("payload")
                .long("string")
                .short('s')
                .help("inline payload string.")
                .required(false)
                .value_name("PAYLOAD")
                .conflicts_with("path"),
        )
}
