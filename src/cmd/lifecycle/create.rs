// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::io::reduct::build_client;
use crate::parse::widely_used_args::{make_entries_arg, make_when_arg};
use crate::parse::{Resource, ResourcePathParser};
use clap::{Arg, Command};
use reduct_rs::{LifecycleSettings, LifecycleType};

pub(super) fn create_lifecycle_cmd() -> Command {
    Command::new("create")
        .about("Create a lifecycle policy")
        .arg(
            Arg::new("LIFECYCLE_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("BUCKET_NAME")
                .help("Bucket to apply lifecycle policy")
                .required(true),
        )
        .arg(
            Arg::new("max-age")
                .long("max-age")
                .value_name("DURATION")
                .help("Maximum record age (e.g. 1h, 30d, 3600s)")
                .required(true),
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

pub(super) async fn create_lifecycle(
    ctx: &crate::context::CliContext,
    args: &clap::ArgMatches,
) -> anyhow::Result<()> {
    let (alias_or_url, lifecycle_name) = args
        .get_one::<Resource>("LIFECYCLE_PATH")
        .unwrap()
        .clone()
        .pair()?;
    let bucket_name = args.get_one::<String>("BUCKET_NAME").unwrap();
    let max_age = args.get_one::<String>("max-age").unwrap();
    let interval = args.get_one::<String>("interval");
    let entries = args
        .get_many::<String>("entries")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    let when = args.get_one::<String>("when");

    let client = build_client(ctx, &alias_or_url).await?;

    let mut settings = LifecycleSettings::default();
    settings.lifecycle_type = LifecycleType::Delete;
    settings.bucket = bucket_name.to_string();
    settings.max_age = max_age.to_string();
    settings.entries = entries;
    if let Some(interval) = interval {
        settings.interval = interval.to_string();
    }
    if let Some(when) = when {
        settings.when = Some(serde_json::from_str(when)?);
    }

    client
        .create_lifecycle(&lifecycle_name)
        .set_settings(settings)
        .send()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::lifecycle::tests::unique_name;
    use crate::context::tests::context;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[tokio::test]
    async fn test_create_lifecycle(context: crate::context::CliContext) {
        let test_lifecycle = unique_name("test-lifecycle");
        let bucket = unique_name("test-bucket");

        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket).send().await.unwrap();

        let args = create_lifecycle_cmd().get_matches_from(vec![
            "create",
            format!("local/{}", test_lifecycle).as_str(),
            bucket.as_str(),
            "--max-age",
            "1h",
            "--interval",
            "10m",
            "--entries",
            "entry1",
            "entry2",
            "--when",
            r#"{"&label": {"$gt": 10}}"#,
        ]);
        create_lifecycle(&context, &args).await.unwrap();

        let lifecycle = client.get_lifecycle(&test_lifecycle).await.unwrap();
        assert_eq!(lifecycle.settings.bucket, bucket);
        assert_eq!(lifecycle.settings.max_age, "1h");
        assert_eq!(lifecycle.settings.interval, "10m");
        assert_eq!(lifecycle.settings.entries, vec!["entry1", "entry2"]);
        assert_eq!(
            lifecycle.settings.when.unwrap(),
            json!({"&label": {"$gt": 10}})
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_lifecycle_without_interval(context: crate::context::CliContext) {
        let test_lifecycle = unique_name("test-lifecycle");
        let bucket = unique_name("test-bucket");

        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket).send().await.unwrap();

        let args = create_lifecycle_cmd().get_matches_from(vec![
            "create",
            format!("local/{}", test_lifecycle).as_str(),
            bucket.as_str(),
            "--max-age",
            "1h",
        ]);
        create_lifecycle(&context, &args).await.unwrap();

        let lifecycle = client.get_lifecycle(&test_lifecycle).await.unwrap();
        assert_eq!(lifecycle.settings.interval, "3600s");
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_lifecycle_with_invalid_alias() {
        let args = create_lifecycle_cmd().try_get_matches_from(vec![
            "create",
            "invalid",
            "test_bucket",
            "--max-age",
            "1h",
        ]);
        assert!(args.is_err());
    }
}
