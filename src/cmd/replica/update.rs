// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::replica::make_prefix_arg;
use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::io::reduct::{build_client, parse_url_and_token};
use crate::parse::widely_used_args::{make_compression_arg, make_entries_arg, make_when_arg};
use crate::parse::{Resource, ResourcePathParser};

use clap::{Arg, ArgMatches, Command};
use reduct_rs::{ReplicationCompression, ReplicationSettings};

pub(super) fn update_replica_cmd() -> Command {
    Command::new("update")
        .about("Update a replication task")
        .arg(
            Arg::new("REPLICATION_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("DEST_BUCKET_PATH")
                .help(RESOURCE_PATH_HELP)
                .required(true)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("source-bucket")
                .long("source-bucket")
                .short('s')
                .help("Source bucket on the replicated instance")
                .required(false),
        )
        .arg(make_entries_arg())
        .arg(make_prefix_arg())
        .arg(make_when_arg())
        .arg(make_compression_arg())
}

pub(super) async fn update_replica_handler(
    ctx: &CliContext,
    args: &ArgMatches,
) -> anyhow::Result<()> {
    let (alias_or_url, replication_name) = args
        .get_one::<Resource>("REPLICATION_PATH")
        .unwrap()
        .clone()
        .pair()?;

    let client = build_client(ctx, &alias_or_url).await?;
    let current_settings = client.get_replication(&replication_name).await?.settings;
    let new_settings = update_replication_settings(ctx, args, current_settings)?;

    client
        .update_replication(&replication_name, new_settings)
        .await?;
    Ok(())
}

fn update_replication_settings(
    ctx: &CliContext,
    args: &ArgMatches,
    mut current_settings: ReplicationSettings,
) -> anyhow::Result<ReplicationSettings> {
    // we require the destination bucket path because it is the only way to obtain the access token
    let (dest_alias_or_url, dest_bucket_name) = args
        .get_one::<Resource>("DEST_BUCKET_PATH")
        .unwrap()
        .clone()
        .pair()?;

    let source_bucket_name = args.get_one::<String>("source-bucket");

    let entries_filter = args
        .get_many::<String>("entries")
        .map(|s| s.map(|s| s.to_string()).collect::<Vec<String>>());

    let prefix = args.get_one::<String>("prefix");
    let when = args.get_one::<String>("when");
    let compression = args.get_one::<ReplicationCompression>("compression");

    let (dest_url, token) = parse_url_and_token(ctx, &dest_alias_or_url)?;
    current_settings.dst_bucket = dest_bucket_name.clone();
    current_settings.dst_host = dest_url.to_string();
    current_settings.dst_token = Some(token.clone());

    if let Some(source_bucket_name) = source_bucket_name {
        current_settings.src_bucket = source_bucket_name.clone();
    }

    if let Some(entries_filter) = entries_filter {
        current_settings.entries = entries_filter;
    }

    if let Some(prefix) = prefix {
        current_settings.dst_prefix = prefix.clone();
    }

    if let Some(when) = when {
        current_settings.when = serde_json::from_str(&when)?;
    }

    if let Some(compression) = compression {
        current_settings.compression = compression.clone();
    }

    Ok(current_settings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::replica::tests::prepare_replication;
    use crate::context::tests::{bucket, bucket2, context, replica};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_update_replica(
        context: CliContext,
        #[future] replica: String,
        #[future] bucket: String,
        #[future] bucket2: String,
    ) {
        let test_replica = replica.await;
        let bucket = bucket.await;
        let bucket2 = bucket2.await;

        let client = prepare_replication(&context, &test_replica, &bucket, &bucket2)
            .await
            .unwrap();

        let args = update_replica_cmd()
            .try_get_matches_from(vec![
                "update",
                format!("local/{}", test_replica).as_str(),
                format!("local/{}", &bucket2).as_str(),
                "--prefix",
                "robot-2",
                "--compression",
                "zstd",
            ])
            .unwrap();

        update_replica_handler(&context, &args).await.unwrap();

        let replica = client.get_replication(&test_replica).await.unwrap();
        assert_eq!(replica.settings.src_bucket, bucket);
        assert_eq!(replica.settings.dst_bucket, bucket2);
        assert_eq!(replica.settings.dst_host, "http://localhost:8383/");
        assert_eq!(
            replica.settings.dst_token.unwrap_or("***".into()),
            "***",
            "Keep compatibility with v1.16"
        );
        assert!(replica.settings.entries.is_empty());
        assert_eq!(replica.settings.dst_prefix, "robot-2");
        assert_eq!(replica.settings.compression, ReplicationCompression::Zstd);
    }

    #[test]
    fn test_update_replica_with_prefix() {
        let args = update_replica_cmd()
            .try_get_matches_from([
                "update",
                "local/test_replica",
                "local/destination",
                "--prefix",
                "robot-2",
            ])
            .unwrap();

        assert_eq!(args.get_one::<String>("prefix").unwrap(), "robot-2");
    }

    #[rstest]
    #[case("zstd", ReplicationCompression::Zstd)]
    #[case("gzip", ReplicationCompression::Gzip)]
    #[case("none", ReplicationCompression::None)]
    #[tokio::test]
    async fn test_update_replica_compression_methods(
        context: CliContext,
        #[future] replica: String,
        #[future] bucket: String,
        #[future] bucket2: String,
        #[case] compression: &str,
        #[case] expected: ReplicationCompression,
    ) {
        let test_replica = replica.await;
        let bucket = bucket.await;
        let bucket2 = bucket2.await;

        let client = prepare_replication(&context, &test_replica, &bucket, &bucket2)
            .await
            .unwrap();

        let args = update_replica_cmd()
            .try_get_matches_from(vec![
                "update",
                format!("local/{}", test_replica).as_str(),
                format!("local/{}", &bucket2).as_str(),
                "--compression",
                compression,
            ])
            .unwrap();

        update_replica_handler(&context, &args).await.unwrap();

        let replica = client.get_replication(&test_replica).await.unwrap();
        assert_eq!(replica.settings.compression, expected);
    }

    #[test]
    fn test_update_replica_with_invalid_compression() {
        let args = update_replica_cmd().try_get_matches_from([
            "update",
            "local/test_replica",
            "local/destination",
            "--compression",
            "invalid",
        ]);
        assert!(args.is_err());
        let err = args.unwrap_err();
        assert!(err.to_string().contains("invalid compression method"));
    }

    mod update_settings {
        use super::*;
        use crate::context::tests::current_token;
        use rstest::fixture;

        #[rstest]
        fn override_src_bucket(context: CliContext, current_settings: ReplicationSettings) {
            let args = update_replica_cmd()
                .try_get_matches_from(vec![
                    "update",
                    "local/test_replica",
                    "local/bucket2",
                    "--source-bucket",
                    "bucket3",
                ])
                .unwrap();

            let new_settings =
                update_replication_settings(&context, &args, current_settings.clone()).unwrap();
            assert_eq!(
                new_settings,
                ReplicationSettings {
                    src_bucket: "bucket3".to_string(),
                    compression: ReplicationCompression::None,
                    ..current_settings
                }
            );
        }

        #[rstest]
        fn override_dest_path(
            context: CliContext,
            current_settings: ReplicationSettings,
            current_token: String,
        ) {
            let args = update_replica_cmd()
                .try_get_matches_from(vec!["update", "local/test_replica", "local/bucket3"])
                .unwrap();

            let new_settings =
                update_replication_settings(&context, &args, current_settings.clone()).unwrap();
            assert_eq!(
                new_settings,
                ReplicationSettings {
                    dst_bucket: "bucket3".to_string(),
                    dst_host: "http://localhost:8383/".to_string(),
                    dst_token: Some(current_token),
                    compression: ReplicationCompression::None,
                    ..current_settings
                }
            );
        }

        #[rstest]
        fn override_entries(context: CliContext, current_settings: ReplicationSettings) {
            let args = update_replica_cmd()
                .try_get_matches_from(vec![
                    "update",
                    "local/test_replica",
                    "local/bucket2",
                    "--entries",
                    "entry3",
                ])
                .unwrap();

            let new_settings =
                update_replication_settings(&context, &args, current_settings.clone()).unwrap();
            assert_eq!(
                new_settings,
                ReplicationSettings {
                    entries: vec!["entry3".to_string()],
                    compression: ReplicationCompression::None,
                    ..current_settings
                }
            );
        }

        #[rstest]
        fn override_when(context: CliContext, current_settings: ReplicationSettings) {
            let args = update_replica_cmd()
                .try_get_matches_from(vec![
                    "update",
                    "local/test_replica",
                    "local/bucket2",
                    "--when",
                    r#"{"&label": {"$gt": 20}}"#,
                ])
                .unwrap();

            let new_settings =
                update_replication_settings(&context, &args, current_settings.clone()).unwrap();
            assert_eq!(
                new_settings,
                ReplicationSettings {
                    when: Some(serde_json::json!({"&label": {"$gt": 20}})),
                    compression: ReplicationCompression::None,
                    ..current_settings
                }
            );
        }

        #[rstest]
        fn override_prefix(context: CliContext, current_settings: ReplicationSettings) {
            let args = update_replica_cmd()
                .try_get_matches_from([
                    "update",
                    "local/test_replica",
                    "local/bucket2",
                    "--prefix",
                    "robot-2",
                ])
                .unwrap();

            let new_settings =
                update_replication_settings(&context, &args, current_settings.clone()).unwrap();
            assert_eq!(
                new_settings,
                ReplicationSettings {
                    dst_prefix: "robot-2".to_string(),
                    compression: ReplicationCompression::None,
                    ..current_settings
                }
            );
        }

        #[rstest]
        fn preserve_prefix_when_omitted(
            context: CliContext,
            current_settings: ReplicationSettings,
        ) {
            let args = update_replica_cmd()
                .try_get_matches_from(["update", "local/test_replica", "local/bucket2"])
                .unwrap();

            let new_settings =
                update_replication_settings(&context, &args, current_settings).unwrap();
            assert_eq!(new_settings.dst_prefix, "robot-1");
        }

        #[rstest]
        #[case("zstd", ReplicationCompression::Zstd)]
        #[case("gzip", ReplicationCompression::Gzip)]
        #[case("none", ReplicationCompression::None)]
        fn override_compression(
            context: CliContext,
            current_settings: ReplicationSettings,
            #[case] compression: &str,
            #[case] expected: ReplicationCompression,
        ) {
            let args = update_replica_cmd()
                .try_get_matches_from(vec![
                    "update",
                    "local/test_replica",
                    "local/bucket2",
                    "--compression",
                    compression,
                ])
                .unwrap();

            let new_settings =
                update_replication_settings(&context, &args, current_settings).unwrap();
            assert_eq!(new_settings.compression, expected);
        }

        #[rstest]
        fn preserve_compression_when_omitted(
            context: CliContext,
            current_settings: ReplicationSettings,
        ) {
            let args = update_replica_cmd()
                .try_get_matches_from(["update", "local/test_replica", "local/bucket2"])
                .unwrap();

            let new_settings =
                update_replication_settings(&context, &args, current_settings).unwrap();
            assert_eq!(new_settings.compression, ReplicationCompression::None);
        }

        #[fixture]
        fn current_settings(current_token: String) -> ReplicationSettings {
            ReplicationSettings {
                src_bucket: "bucket1".to_string(),
                dst_bucket: "bucket2".to_string(),
                dst_host: "http://localhost:8383/".to_string(),
                dst_token: Some(current_token),
                dst_prefix: "robot-1".to_string(),
                entries: vec!["entry1".to_string(), "entry2".to_string()],
                when: None,
                mode: Default::default(),
                compression: ReplicationCompression::Gzip,
                ..Default::default()
            }
        }
    }
}
