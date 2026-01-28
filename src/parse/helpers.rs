// Copyright 2024-2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::CliContext;
use chrono::DateTime;
use clap::parser::MatchesError;
use clap::ArgMatches;
use reduct_rs::{Bucket, EntryInfo};
use serde_json::Value;
use std::time::Duration;

pub(crate) async fn fetch_and_filter_entries(
    src_bucket: &Bucket,
    entry_filter: &Vec<String>,
) -> anyhow::Result<Vec<EntryInfo>> {
    let entries = src_bucket
        .entries()
        .await?
        .iter()
        .filter(|entry| -> bool {
            if entry_filter.is_empty() || entry_filter.contains(&entry.name) {
                true
            } else {
                // check wildcard
                entry_filter.iter().any(|filter| {
                    filter.ends_with('*') && entry.name.starts_with(&filter[..filter.len() - 1])
                })
            }
        })
        .map(|entry| entry.clone())
        .collect::<Vec<EntryInfo>>();
    Ok(entries)
}

pub(crate) fn parse_time(time_str: Option<&String>) -> anyhow::Result<Option<u64>> {
    if time_str.is_none() {
        return Ok(None);
    }

    let time_str = time_str.unwrap();
    let time = if let Ok(time) = time_str.parse::<i64>() {
        if time < 0 {
            return Err(anyhow::anyhow!("Time must be a positive integer"));
        }
        time
    } else {
        // try parse as ISO 8601
        DateTime::parse_from_rfc3339(time_str)
            .map_err(|err| anyhow::anyhow!("Failed to parse time {}: {}", time_str, err))?
            .timestamp_micros()
    };

    Ok(Some(time as u64))
}

#[derive(Clone, Debug)]
pub(crate) struct QueryParams {
    pub start: Option<u64>,
    pub stop: Option<u64>,
    pub each_n: Option<u64>,
    pub each_s: Option<f64>,
    pub limit: Option<u64>,
    pub entry_filter: Vec<String>,
    pub parallel: usize,
    pub ttl: Duration,
    pub when: Option<Value>,
    pub strict: bool,
    pub ext: Option<Value>,
    pub quiet: bool,
}

impl Default for QueryParams {
    fn default() -> Self {
        Self {
            start: None,
            stop: None,
            each_n: None,
            each_s: None,
            limit: None,
            entry_filter: Vec::new(),
            parallel: 1,
            ttl: Duration::from_secs(60),
            when: None,
            strict: false,
            ext: None,
            quiet: false,
        }
    }
}

pub(crate) fn parse_query_params(
    ctx: &CliContext,
    args: &ArgMatches,
) -> anyhow::Result<QueryParams> {
    let start = parse_time(args.get_one::<String>("start"))?;
    let stop = parse_time(args.get_one::<String>("stop"))?;
    let each_n = args.get_one::<u64>("each-n").map(|n| *n);
    let each_s = args.get_one::<f64>("each-s").map(|s| *s);
    let when = args.get_one::<String>("when").map(|s| s.to_string());
    let strict = args.get_one::<bool>("strict").unwrap_or(&false);
    let quiet = args.get_flag("quiet");
    let ext_params = match args.try_get_one::<String>("ext-params") {
        Ok(s) => s.cloned(),
        Err(MatchesError::UnknownArgument { .. }) => None, // ext-params is optional for some commands
        Err(err) => return Err(anyhow::anyhow!("Failed to parse ext-params: {}", err)),
    };

    // arguments aren't used in all cases
    let limit = args
        .try_get_one::<u64>("limit")
        .ok()
        .unwrap_or(None)
        .map(|n| *n);

    let entries_filter = args
        .get_many::<String>("entries")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let json_when = match when {
        Some(when) => {
            let when = match serde_json::from_str(&when) {
                Ok(when) => when,
                Err(err) => return Err(anyhow::anyhow!("Failed to parse when parameter: {}", err)),
            };
            Some(when)
        }
        None => None,
    };

    let ext_json = match ext_params {
        Some(params) => {
            let ext_json = match serde_json::from_str(&params) {
                Ok(json) => json,
                Err(err) => return Err(anyhow::anyhow!("Failed to parse ext-params: {}", err)),
            };
            Some(Value::Object(ext_json))
        }
        None => None,
    };

    Ok(QueryParams {
        start,
        stop,
        each_n,
        each_s,
        limit,
        entry_filter: entries_filter,
        parallel: ctx.parallel(),
        ttl: ctx.timeout() * ctx.parallel() as u32,
        when: json_when,
        strict: *strict,
        ext: ext_json,
        quiet,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    mod parse_query_params {
        use crate::cmd::cp::cp_cmd;
        use crate::context::tests::context;
        use rstest::rstest;

        use super::*;

        #[rstest]
        fn parse_start_stop(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2"])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.start, None);
            assert_eq!(query_params.stop, None);
        }

        #[rstest]
        fn parse_start_stop_with_values(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    "serv/buck1",
                    "serv/buck2",
                    "--start",
                    "100",
                    "--stop",
                    "200",
                ])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.start, Some(100));
            assert_eq!(query_params.stop, Some(200));
        }

        #[rstest]
        fn parse_start_stop_iso8601(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    "serv/buck1",
                    "serv/buck2",
                    "--start",
                    "2023-01-01T00:00:00Z",
                    "--stop",
                    "2023-01-02T00:00:00Z",
                ])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.start, Some(1672531200000000));
            assert_eq!(query_params.stop, Some(1672617600000000));
        }

        #[rstest]
        fn parse_each_n(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2", "--each-n", "10"])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.each_n, Some(10));
        }

        #[rstest]
        fn parse_each_s(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2", "--each-s", "10"])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.each_s, Some(10.0));
        }

        #[rstest]
        fn parse_limit(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2", "--limit", "100"])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.limit, Some(100));
        }

        #[rstest]
        fn parse_entries_filter(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    "serv/buck1",
                    "serv/buck2",
                    "--entries",
                    "entry-1",
                    "entry-2",
                ])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(
                query_params.entry_filter,
                vec![String::from("entry-1"), String::from("entry-2")]
            );
        }

        #[rstest]
        fn parse_when(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    "serv/buck1",
                    "serv/buck2",
                    "--when",
                    r#"{"$gt": 100}"#,
                ])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.when, Some(serde_json::json!({"$gt": 100})));
        }

        #[rstest]
        fn parse_when_invalid(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    "serv/buck1",
                    "serv/buck2",
                    "--when",
                    r#"{"$gt": 100"#,
                ])
                .unwrap();
            let query_params = parse_query_params(&context, &args);

            assert!(query_params
                .err()
                .unwrap()
                .to_string()
                .contains("Failed to parse when parameter"));
        }

        #[rstest]
        fn parse_strict(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2", "--strict"])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.strict, true);
        }
    }
}
