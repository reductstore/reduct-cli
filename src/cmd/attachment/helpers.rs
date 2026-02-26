// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::builder::TypedValueParser;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Arg, Command, Error};
use reduct_rs::{Bucket, ErrorCode};
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::OsStr;

const ENTRY_PATH_HELP: &str =
    "Path to an entry (e.g. SERVER_ALIAS/BUCKET/ENTRY_NAME or http://token@localhost:8383/BUCKET/ENTRY_NAME)";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct EntryPath {
    pub(super) alias_or_url: String,
    pub(super) bucket: String,
    pub(super) entry: String,
}

#[derive(Clone)]
struct EntryPathParser;

impl TypedValueParser for EntryPathParser {
    type Value = EntryPath;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        let value = value.to_string_lossy().to_string();
        let mut parts = value.rsplitn(3, '/');
        let entry = parts.next().unwrap_or_default();
        let bucket = parts.next().unwrap_or_default();
        let alias_or_url = parts.next().unwrap_or_default();

        if alias_or_url.is_empty() || bucket.is_empty() || entry.is_empty() {
            let mut err = Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
            err.insert(
                ContextKind::InvalidArg,
                ContextValue::String(arg.unwrap().to_string()),
            );
            err.insert(ContextKind::InvalidValue, ContextValue::String(value));
            return Err(err);
        }

        Ok(EntryPath {
            alias_or_url: alias_or_url.to_string(),
            bucket: bucket.to_string(),
            entry: entry.to_string(),
        })
    }
}

pub(super) fn entry_path_arg(name: &'static str) -> Arg {
    Arg::new(name)
        .help(ENTRY_PATH_HELP)
        .value_parser(EntryPathParser)
        .required(true)
}

pub(super) async fn read_attachments_or_empty(
    bucket: &Bucket,
    entry: &str,
) -> anyhow::Result<HashMap<String, Value>> {
    match bucket.read_attachments(entry).await {
        Ok(attachments) => Ok(attachments),
        Err(err) if err.status() == ErrorCode::NotFound => Ok(HashMap::new()),
        Err(err) => Err(err.into()),
    }
}
