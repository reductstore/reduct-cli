// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::io::reduct::{build_client, parse_url_and_token};
use crate::parse::widely_used_args::{make_each_n, make_each_s, make_entries_arg, make_when_arg};
use crate::parse::ResourcePathParser;
use clap::{Arg, Command};
use reduct_rs::ReplicationSettings;

pub(super) fn create_replica_cmd() -> Command {
    Command::new("create")
        .about("Create a replication task")
        .arg(
            Arg::new("REPLICATION_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("SOURCE_BUCKET_NAME")
                .help("Source bucket on the replicated instance")
                .required(true),
        )
        .arg(
            Arg::new("DEST_BUCKET_PATH")
                .help(RESOURCE_PATH_HELP)
                .required(true)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(make_entries_arg())
        .arg(make_each_n())
        .arg(make_each_s())
        .arg(make_when_arg())
}

pub(super) async fn create_replica(
    ctx: &crate::context::CliContext,
    args: &clap::ArgMatches,
) -> anyhow::Result<()> {
    let (alias_or_url, replication_name) = args
        .get_one::<(String, String)>("REPLICATION_PATH")
        .unwrap();
    let source_bucket_name = args.get_one::<String>("SOURCE_BUCKET_NAME").unwrap();
    let (dest_alias_or_url, dest_bucket_name) = args
        .get_one::<(String, String)>("DEST_BUCKET_PATH")
        .unwrap();
    let entries_filter = args
        .get_many::<String>("entries")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let each_n = args.get_one::<u64>("each-n");
    let each_s = args.get_one::<f64>("each-s");
    let when = args.get_one::<String>("when");

    let client = build_client(ctx, &alias_or_url).await?;
    let (dest_url, token) = parse_url_and_token(ctx, &dest_alias_or_url)?;

    let mut settings = ReplicationSettings::default();
    settings.src_bucket = source_bucket_name.to_string();
    settings.dst_bucket = dest_bucket_name.to_string();
    settings.dst_host = dest_url.as_str().to_string();
    settings.dst_token = Some(token);
    settings.entries = entries_filter;
    settings.each_n = each_n.copied();
    settings.each_s = each_s.copied();

    if let Some(when) = when {
        settings.when = Some(serde_json::from_str(&when)?);
    }
    client
        .create_replication(replication_name)
        .set_settings(settings)
        .send()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, bucket2, context, replica};

    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[tokio::test]
    async fn test_create_replica(
        context: crate::context::CliContext,
        #[future] replica: String,
        #[future] bucket: String,
        #[future] bucket2: String,
    ) {
        let test_replica = replica.await;
        let bucket = bucket.await;
        let bucket2 = bucket2.await;

        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket).send().await.unwrap();
        client.create_bucket(&bucket2).send().await.unwrap();

        let args = create_replica_cmd().get_matches_from(vec![
            "create",
            format!("local/{}", test_replica).as_str(),
            &bucket,
            format!("local/{}", bucket2).as_str(),
            "--entries",
            "entry1",
            "entry2",
            "--each-n",
            "10",
            "--each-s",
            "0.5",
            "--when",
            r#"{"&label": {"$gt": 10}}"#,
        ]);
        create_replica(&context, &args).await.unwrap();

        let replica = client.get_replication(&test_replica).await.unwrap();
        assert_eq!(replica.settings.src_bucket, bucket);
        assert_eq!(replica.settings.dst_bucket, bucket2);
        assert_eq!(replica.settings.dst_host, "http://localhost:8383/");
        assert_eq!(
            replica.settings.dst_token.unwrap_or("***".into()),
            "***",
            "Keep compatibility with v1.16"
        );
        assert_eq!(replica.settings.each_n, Some(10));
        assert_eq!(replica.settings.each_s, Some(0.5));
        assert_eq!(
            replica.settings.when.unwrap(),
            json!({"&label": {"$gt": 10}})
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_replica_with_invalid_alias() {
        let args = create_replica_cmd().try_get_matches_from(vec![
            "create",
            "invalid",
            "test_bucket",
            "local/test_bucket_2",
        ]);
        assert!(args.is_err());
    }
}
