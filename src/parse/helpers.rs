// Copyright 2024-2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::{resolve_connection_options, ConnectionOptions};
use crate::context::CliContext;
use chrono::{DateTime, Local, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Utc};
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
    let time_str = match time_str {
        Some(value) => value.trim(),
        None => return Ok(None),
    };

    if time_str.is_empty() {
        return Err(anyhow::anyhow!("Time is empty"));
    }

    if time_str.starts_with('-') {
        return Err(anyhow::anyhow!("Time must be a positive integer"));
    }

    // Treat digit-only values as timestamps so numeric overflow does not fall through
    // into date parsing and produce a misleading format error.
    if time_str.chars().all(|c| c.is_ascii_digit()) {
        match time_str.parse::<u64>() {
            Ok(value) => return Ok(Some(value)),
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "Failed to parse numeric timestamp {}: {}",
                    time_str,
                    err
                ))
            }
        }
    }

    // Keep timezone-aware input first so explicit offsets always win over local-time
    // interpretation.
    if let Ok(value) = DateTime::parse_from_rfc3339(time_str) {
        let timestamp = timestamp_micros_to_u64(value.timestamp_micros(), time_str)?;
        return Ok(Some(timestamp));
    }

    if let Some(timestamp) = parse_relative_time(time_str)? {
        return Ok(Some(timestamp));
    }

    // Timezone-free input is resolved through the machine's local timezone because
    // users asked for short local date/time forms.
    if let Ok(value) = NaiveDateTime::parse_from_str(time_str, "%Y-%m-%dT%H:%M:%S") {
        let timestamp = local_datetime_to_timestamp(value, time_str)?;
        return Ok(Some(timestamp));
    }

    if let Ok(value) = NaiveDate::parse_from_str(time_str, "%Y-%m-%d") {
        let value = value
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow::anyhow!("Failed to parse time {}", time_str))?;

        let timestamp = local_datetime_to_timestamp(value, time_str)?;
        return Ok(Some(timestamp));
    }

    Err(anyhow::anyhow!(
        "Failed to parse time {}: expected Unix microseconds, RFC3339, local datetime, or local date",
        time_str
    ))
}

fn parse_relative_time(time_str: &str) -> anyhow::Result<Option<u64>> {
    let compact = time_str
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect::<String>();
    if compact == "now" {
        return Ok(Some(Utc::now().timestamp_micros() as u64));
    }

    let (operator, duration) = match compact.as_bytes().get(3) {
        Some(b'-') => ('-', &compact[4..]),
        Some(b'+') => ('+', &compact[4..]),
        _ => return Ok(None),
    };
    if !compact.starts_with("now") || duration.is_empty() {
        return Ok(None);
    }

    let unit_start = duration
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(duration.len());
    if unit_start == 0 {
        return Ok(None);
    }

    let amount = duration[..unit_start]
        .parse::<i64>()
        .map_err(|err| anyhow::anyhow!("Failed to parse relative time {}: {}", time_str, err))?;
    let multiplier = match &duration[unit_start..] {
        "ms" => 1_000,
        "s" => 1_000_000,
        "m" => 60_000_000,
        "h" => 3_600_000_000,
        "d" => 86_400_000_000,
        "w" => 604_800_000_000,
        _ => return Ok(None),
    };
    let offset = amount.checked_mul(multiplier).ok_or_else(|| {
        anyhow::anyhow!(
            "Failed to parse relative time {}: duration is too large",
            time_str
        )
    })?;
    let now = Utc::now().timestamp_micros();
    let timestamp = match operator {
        '-' => now.checked_sub(offset),
        '+' => now.checked_add(offset),
        _ => unreachable!(),
    }
    .ok_or_else(|| {
        anyhow::anyhow!(
            "Failed to parse relative time {}: timestamp is out of range",
            time_str
        )
    })?;

    Ok(Some(timestamp_micros_to_u64(timestamp, time_str)?))
}

fn local_datetime_to_timestamp(value: NaiveDateTime, time_str: &str) -> anyhow::Result<u64> {
    match Local.from_local_datetime(&value) {
        LocalResult::Single(value) => timestamp_micros_to_u64(value.timestamp_micros(), time_str),
        // Ambiguous/nonexistent local times happen around DST transitions; fail loudly
        // instead of silently choosing the wrong instant.
        LocalResult::Ambiguous(_, _) => Err(anyhow::anyhow!(
            "Failed to parse time {}: local time is ambiguous",
            time_str
        )),
        LocalResult::None => Err(anyhow::anyhow!(
            "Failed to parse time {}: local time does not exist",
            time_str
        )),
    }
}

fn timestamp_micros_to_u64(micros: i64, time_str: &str) -> anyhow::Result<u64> {
    u64::try_from(micros).map_err(|_| {
        anyhow::anyhow!(
            "Timestamp must be greater than or equal to Unix epoch: {}",
            time_str
        )
    })
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
    alias_or_url: Option<&str>,
) -> anyhow::Result<QueryParams> {
    let start = parse_time(args.get_one::<String>("start"))?;
    let stop = parse_time(args.get_one::<String>("stop"))?
        .or_else(|| Some(Utc::now().timestamp_micros() as u64));
    let each_n = args.get_one::<u64>("each-n").map(|n| *n);
    let each_s = args.get_one::<f64>("each-s").map(|s| *s);
    let when = args.get_one::<String>("when").map(|s| s.to_string());
    let strict = args.get_one::<bool>("strict").unwrap_or(&false);
    let quiet = match args.try_get_one::<bool>("quiet") {
        Ok(Some(value)) => *value,
        Ok(None) => false,
        Err(MatchesError::UnknownArgument { .. }) => false,
        Err(err) => return Err(anyhow::anyhow!("Failed to parse quiet flag: {}", err)),
    };
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

    let options = alias_or_url.map_or_else(
        || ConnectionOptions {
            ignore_ssl: ctx.ignore_ssl().unwrap_or(false),
            timeout: ctx.timeout().unwrap_or(crate::context::DEFAULT_TIMEOUT),
            parallel: ctx.parallel().unwrap_or(crate::context::DEFAULT_PARALLEL),
            ca_cert: ctx.ca_cert().cloned(),
        },
        |endpoint| resolve_connection_options(ctx, endpoint),
    );

    Ok(QueryParams {
        start,
        stop,
        each_n,
        each_s,
        limit,
        entry_filter: entries_filter,
        parallel: options.parallel,
        ttl: options.timeout * options.parallel as u32,
        when: json_when,
        strict: *strict,
        ext: ext_json,
        quiet,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    mod parse_time_tests {
        use super::*;

        #[test]
        fn returns_none_when_time_is_not_provided() {
            assert_eq!(parse_time(None).unwrap(), None);
        }

        #[test]
        fn rejects_empty_time() {
            assert_eq!(
                parse_time(Some(&"   ".to_string()))
                    .err()
                    .unwrap()
                    .to_string(),
                "Time is empty"
            );
        }

        #[test]
        fn rejects_negative_numeric_timestamp() {
            assert_eq!(
                parse_time(Some(&"-100".to_string()))
                    .err()
                    .unwrap()
                    .to_string(),
                "Time must be a positive integer"
            );
        }

        #[test]
        fn parses_numeric_timestamp_with_surrounding_whitespace() {
            assert_eq!(parse_time(Some(&" 100 ".to_string())).unwrap(), Some(100));
        }

        #[test]
        fn parses_relative_time_without_spaces() {
            let before = (Utc::now().timestamp_micros() - 3_600_000_000) as u64;
            let parsed = parse_time(Some(&"now-1h".to_string())).unwrap().unwrap();
            let after = (Utc::now().timestamp_micros() - 3_600_000_000) as u64;

            assert!(parsed >= before && parsed <= after);
        }

        #[test]
        fn parses_relative_time_with_spaces_and_units() {
            let before = (Utc::now().timestamp_micros() + 60_000_000) as u64;
            let parsed = parse_time(Some(&"now + 1m".to_string())).unwrap().unwrap();
            let after = (Utc::now().timestamp_micros() + 60_000_000) as u64;

            assert!(parsed >= before && parsed <= after);
        }

        #[test]
        fn rejects_numeric_timestamp_overflow() {
            assert!(
                parse_time(Some(&"999999999999999999999999999999".to_string()))
                    .err()
                    .unwrap()
                    .to_string()
                    .starts_with(
                        "Failed to parse numeric timestamp 999999999999999999999999999999"
                    )
            );
        }

        #[test]
        fn parses_rfc3339_timestamp() {
            assert_eq!(
                parse_time(Some(&"2023-01-01T00:00:00Z".to_string())).unwrap(),
                Some(1672531200000000)
            );
        }

        #[test]
        fn parses_rfc3339_timestamp_with_offset() {
            assert_eq!(
                parse_time(Some(&"2023-01-01T03:00:00+03:00".to_string())).unwrap(),
                Some(1672531200000000)
            );
        }

        #[test]
        fn parses_local_datetime() {
            let expected = Local
                .with_ymd_and_hms(2026, 1, 1, 20, 0, 0)
                .single()
                .unwrap()
                .timestamp_micros() as u64;

            assert_eq!(
                parse_time(Some(&"2026-01-01T20:00:00".to_string())).unwrap(),
                Some(expected)
            );
        }

        #[test]
        fn rejects_local_datetime_with_space_separator() {
            assert_eq!(
                parse_time(Some(&"2026-01-01 20:00:00".to_string()))
                    .err()
                    .unwrap()
                    .to_string(),
                "Failed to parse time 2026-01-01 20:00:00: expected Unix microseconds, RFC3339, local datetime, or local date"
            );
        }

        #[test]
        fn rejects_local_datetime_with_fractional_seconds() {
            assert_eq!(
                parse_time(Some(&"2026-01-01T20:00:00.123456".to_string()))
                    .err()
                    .unwrap()
                    .to_string(),
                "Failed to parse time 2026-01-01T20:00:00.123456: expected Unix microseconds, RFC3339, local datetime, or local date"
            );
        }

        #[test]
        fn parses_local_date() {
            let expected = Local
                .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
                .single()
                .unwrap()
                .timestamp_micros() as u64;

            assert_eq!(
                parse_time(Some(&"2026-01-01".to_string())).unwrap(),
                Some(expected)
            );
        }

        #[test]
        fn parses_local_date_without_zero_padding() {
            let expected = Local
                .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
                .single()
                .unwrap()
                .timestamp_micros() as u64;

            assert_eq!(
                parse_time(Some(&"2026-1-1".to_string())).unwrap(),
                Some(expected)
            );
        }

        #[test]
        fn rejects_invalid_time() {
            assert_eq!(
                parse_time(Some(&"xxx".to_string()))
                    .err()
                    .unwrap()
                    .to_string(),
                "Failed to parse time xxx: expected Unix microseconds, RFC3339, local datetime, or local date"
            );
        }

        #[test]
        fn rejects_pre_epoch_rfc3339_timestamp() {
            assert_eq!(
                parse_time(Some(&"1969-12-31T23:59:59Z".to_string()))
                    .err()
                    .unwrap()
                    .to_string(),
                "Timestamp must be greater than or equal to Unix epoch: 1969-12-31T23:59:59Z"
            );
        }

        #[test]
        fn rejects_pre_epoch_local_date() {
            assert_eq!(
                parse_time(Some(&"1969-12-31".to_string()))
                    .err()
                    .unwrap()
                    .to_string(),
                "Timestamp must be greater than or equal to Unix epoch: 1969-12-31"
            );
        }
    }

    mod parse_query_params {
        use crate::cmd::cp::cp_cmd;
        use crate::config::ConfigFile;
        use crate::context::tests::context;
        use rstest::rstest;

        use super::*;

        #[rstest]
        fn parse_start_stop(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2"])
                .unwrap();
            let before = Utc::now().timestamp_micros() as u64;
            let query_params = parse_query_params(&context, &args, None).unwrap();
            let after = Utc::now().timestamp_micros() as u64;

            assert_eq!(query_params.start, None);
            assert!(query_params.stop.unwrap() >= before);
            assert!(query_params.stop.unwrap() <= after);
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
            let query_params = parse_query_params(&context, &args, None).unwrap();

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
            let query_params = parse_query_params(&context, &args, None).unwrap();

            assert_eq!(query_params.start, Some(1672531200000000));
            assert_eq!(query_params.stop, Some(1672617600000000));
        }

        #[rstest]
        fn parse_each_n(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2", "--each-n", "10"])
                .unwrap();
            let query_params = parse_query_params(&context, &args, None).unwrap();

            assert_eq!(query_params.each_n, Some(10));
        }

        #[rstest]
        fn parse_each_s(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2", "--each-s", "10"])
                .unwrap();
            let query_params = parse_query_params(&context, &args, None).unwrap();

            assert_eq!(query_params.each_s, Some(10.0));
        }

        #[rstest]
        fn parse_limit(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2", "--limit", "100"])
                .unwrap();
            let query_params = parse_query_params(&context, &args, None).unwrap();

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
            let query_params = parse_query_params(&context, &args, None).unwrap();

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
            let query_params = parse_query_params(&context, &args, None).unwrap();

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
            let query_params = parse_query_params(&context, &args, None);

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
            let query_params = parse_query_params(&context, &args, None).unwrap();

            assert_eq!(query_params.strict, true);
        }

        #[rstest]
        fn parse_with_alias_defaults(context: CliContext) {
            let mut config_file = ConfigFile::load(context.config_path()).unwrap();
            let alias = config_file.mut_config().aliases.get_mut("default").unwrap();
            alias.timeout = 12;
            alias.parallel = 3;
            config_file.save().unwrap();

            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "default/buck1", "default/buck2"])
                .unwrap();
            let query_params = parse_query_params(&context, &args, Some("default")).unwrap();

            assert_eq!(query_params.parallel, 3);
            assert_eq!(query_params.ttl, Duration::from_secs(36));
        }
    }
}
