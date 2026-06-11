// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::io::reduct::build_client;
use crate::parse::widely_used_args::{make_entries_arg, make_when_arg};
use crate::parse::{Resource, ResourcePathParser};
use clap::{Arg, Command};
use reduct_rs::LifecycleType;

fn parse_lifecycle_type(value: &str) -> Result<LifecycleType, String> {
    match value {
        "delete" => Ok(LifecycleType::Delete),
        "compress" => Ok(LifecycleType::Compress),
        _ => Err(format!("invalid lifecycle type '{value}'")),
    }
}

pub(super) fn update_lifecycle_cmd() -> Command {
    Command::new("update")
        .about("Update a lifecycle policy")
        .arg(
            Arg::new("LIFECYCLE_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("bucket")
                .long("bucket")
                .value_name("BUCKET_NAME")
                .help("Bucket to apply lifecycle policy")
                .required(false),
        )
        .arg(
            Arg::new("type")
                .long("type")
                .value_name("ACTION")
                .help("Lifecycle action type: delete or compress")
                .value_parser(parse_lifecycle_type)
                .required(false),
        )
        .arg(
            Arg::new("older-than")
                .long("older-than")
                .value_name("DURATION")
                .help("Match records older than this duration (e.g. 1h, 30d, 3600s)")
                .required(false),
        )
        .arg(
            Arg::new("interval")
                .long("interval")
                .value_name("DURATION")
                .help("Interval between lifecycle runs (e.g. 10m, 1h)")
                .required(false),
        )
        .arg(make_entries_arg())
        .arg(make_when_arg())
}

pub(super) async fn update_lifecycle_handler(
    ctx: &crate::context::CliContext,
    args: &clap::ArgMatches,
) -> anyhow::Result<()> {
    let (alias_or_url, lifecycle_name) = args
        .get_one::<Resource>("LIFECYCLE_PATH")
        .unwrap()
        .clone()
        .pair()?;

    let client = build_client(ctx, &alias_or_url).await?;
    let mut settings = client.get_lifecycle(&lifecycle_name).await?.settings;

    if let Some(bucket) = args.get_one::<String>("bucket") {
        settings.bucket = bucket.to_string();
    }
    if let Some(lifecycle_type) = args.get_one::<LifecycleType>("type") {
        settings.lifecycle_type = *lifecycle_type;
    }
    if let Some(older_than) = args.get_one::<String>("older-than") {
        settings.older_than = older_than.to_string();
    }
    if let Some(interval) = args.get_one::<String>("interval") {
        settings.interval = interval.to_string();
    }
    if let Some(entries) = args.get_many::<String>("entries") {
        settings.entries = entries.map(|s| s.to_string()).collect();
    }
    if let Some(when) = args.get_one::<String>("when") {
        settings.when = Some(serde_json::from_str(when)?);
    }

    client.update_lifecycle(&lifecycle_name, settings).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::lifecycle::tests::{prepare_lifecycle, unique_name};
    use crate::context::tests::context;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[tokio::test]
    async fn test_update_lifecycle(context: crate::context::CliContext) {
        let test_lifecycle = unique_name("test-lifecycle");
        let bucket = unique_name("test-bucket");
        let bucket2 = unique_name("test-bucket");

        let client = prepare_lifecycle(&context, &test_lifecycle, &bucket)
            .await
            .unwrap();
        client.create_bucket(&bucket2).send().await.unwrap();

        let args = update_lifecycle_cmd().get_matches_from(vec![
            "update",
            format!("local/{}", test_lifecycle).as_str(),
            "--bucket",
            &bucket2,
            "--older-than",
            "2h",
            "--interval",
            "30m",
            "--entries",
            "entry1",
            "entry2",
            "--when",
            r#"{"&label": {"$eq": 1}}"#,
        ]);
        update_lifecycle_handler(&context, &args).await.unwrap();

        let lifecycle = client.get_lifecycle(&test_lifecycle).await.unwrap();
        assert_eq!(lifecycle.settings.lifecycle_type, LifecycleType::Delete);
        assert_eq!(lifecycle.settings.bucket, bucket2);
        assert_eq!(lifecycle.settings.older_than, "2h");
        assert_eq!(lifecycle.settings.interval, "30m");
        assert_eq!(lifecycle.settings.entries, vec!["entry1", "entry2"]);
        assert_eq!(
            lifecycle.settings.when.unwrap(),
            json!({"&label": {"$eq": 1}})
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_lifecycle_invalid_path() {
        let args = update_lifecycle_cmd().try_get_matches_from(vec!["update", "local"]);

        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'local' for '<LIFECYCLE_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }
}
