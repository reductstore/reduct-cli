// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::parser::ValuesRef;
use clap::ArgAction::SetTrue;
use clap::{value_parser, Arg};
use reduct_rs::Labels;

pub(crate) fn make_include_arg() -> Arg {
    Arg::new("include")
        .long("include")
        .short('I')
        .value_name("KEY=VALUE")
        .help("Deprecated. Use --when instead.\nThese records which have this key-value pair will be requested.\nThe format is key=value. This option can be used multiple times to include multiple key-value pairs.")
        .num_args(0..)
        .required(false)
}

pub(crate) fn make_exclude_arg() -> Arg {
    Arg::new("exclude")
        .long("exclude")
        .short('E')
        .value_name("KEY=VALUE")
        .help("Deprecated. Use --when instead.\nThese records which have this key-value pair will be excluded.\nThe format is key=value. This option can be used multiple times to exclude multiple key-value pairs.")
        .num_args(0..)
        .required(false)
}

pub(crate) fn make_each_n() -> Arg {
    Arg::new("each-n")
        .long("each-n")
        .short('N')
        .help("Export every nth record.\nIf not specified, all records will be requested.")
        .value_name("NUMBER")
        .value_parser(value_parser!(u64))
        .required(false)
}

pub(crate) fn make_each_s() -> Arg {
    Arg::new("each-s")
        .long("each-s")
        .short('S')
        .help("Export a record every n seconds.\nIf not specified, all records will be requested.")
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

pub(crate) fn parse_label_args(args: Option<ValuesRef<String>>) -> anyhow::Result<Option<Labels>> {
    let mut result: Option<Labels> = None;
    if let Some(include_args) = args {
        let mut labels = Labels::new();
        for arg in include_args {
            let (key, value) = parse_label(&arg)?;
            let _ = labels.insert(key, value);
        }

        result = Some(labels);
    }
    Ok(result)
}
