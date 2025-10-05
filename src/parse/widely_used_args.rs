// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::ArgAction::SetTrue;
use clap::{value_parser, Arg};

pub(crate) fn make_each_n() -> Arg {
    Arg::new("each-n")
        .long("each-n")
        .short('N')
        .help("(DEPRECATED) Export every nth record.\nIf not specified, all records will be requested.")
        .value_name("NUMBER")
        .value_parser(value_parser!(u64))
        .required(false)
}

pub(crate) fn make_each_s() -> Arg {
    Arg::new("each-s")
        .long("each-s")
        .short('S')
        .help("(DEPRECATED) Export a record every n seconds.\nIf not specified, all records will be requested.")
        .value_name("NUMBER")
        .value_parser(value_parser!(f64))
        .required(false)
}

pub(crate) fn make_entries_arg() -> Arg {
    Arg::new("entries")
        .long("entries")
        .short('n')
        .value_name("ENTRY_NAME")
        .help("List of entries to export.\nIf not specified, all entries will be requested. Wildcards are supported.")
        .num_args(0..)
        .required(false)
}

pub(crate) fn make_when_arg() -> Arg {
    Arg::new("when")
        .long("when")
        .short('w')
        .value_name("CONDITION")
        .help("Filter records by a condition.\nIf not specified, all records will be requested.")
        .required(false)
}

pub(crate) fn make_strict_arg() -> Arg {
    Arg::new("strict")
        .long("strict")
        .short('s')
        .action(SetTrue)
        .help("If set, `when` will fail if the condition is not met.")
        .required(false)
}

pub(crate) fn make_ext_arg() -> Arg {
    Arg::new("ext-params")
        .long("ext-params")
        .short('X')
        .value_name("EXT_PARAMETERS")
        .help("Pass additional parameters to extensions in JSON format.")
        .required(false)
}

pub(crate) fn parse_label(label: &str) -> anyhow::Result<(String, String)> {
    let mut label = label.splitn(2, '=');
    Ok((
        label
            .next()
            .ok_or(anyhow::anyhow!("Invalid label"))?
            .to_string(),
        label
            .next()
            .ok_or(anyhow::anyhow!("Invalid label"))?
            .to_string(),
    ))
}
