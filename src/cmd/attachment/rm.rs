// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::attachment::helpers::entry_path_arg;
use crate::cmd::attachment::helpers::EntryPath;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgGroup, ArgMatches, Command};
use reduct_rs::ErrorCode;

pub(super) fn rm_attachment_cmd() -> Command {
    Command::new("rm")
        .about("Remove attachments from an entry")
        .arg(entry_path_arg("ENTRY_PATH"))
        .arg(
            Arg::new("KEY")
                .help("Attachment keys to remove")
                .value_name("KEY")
                .num_args(1..)
                .required(false),
        )
        .arg(
            Arg::new("all")
                .long("all")
                .help("Remove all attachments")
                .action(SetTrue)
                .required(false)
                .conflicts_with("KEY"),
        )
        .group(
            ArgGroup::new("remove_target")
                .args(["KEY", "all"])
                .required(true),
        )
}

pub(super) async fn rm_attachment(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let path = args.get_one::<EntryPath>("ENTRY_PATH").unwrap();
    let remove_all = args.get_flag("all");
    let selected_keys = if remove_all {
        None
    } else {
        Some(
            args.get_many::<String>("KEY")
                .unwrap()
                .map(|v| v.to_string())
                .collect::<Vec<String>>(),
        )
    };

    let client = build_client(ctx, &path.alias_or_url).await?;
    let bucket = client.get_bucket(&path.bucket).await?;

    match bucket.remove_attachments(&path.entry, selected_keys).await {
        Ok(_) => {}
        Err(err) if err.status() == ErrorCode::NotFound => {}
        Err(err) => return Err(err.into()),
    }

    if remove_all {
        output!(
            ctx,
            "All attachments removed from '{}/{}/{}'",
            path.alias_or_url,
            path.bucket,
            path.entry
        );
    } else {
        output!(
            ctx,
            "Selected attachments removed from '{}/{}/{}'",
            path.alias_or_url,
            path.bucket,
            path.entry
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::attachment::helpers::read_attachments_or_empty;
    use crate::context::tests::context;
    use crate::io::reduct::build_client;
    use rstest::rstest;
    use serde_json::json;
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_bucket_name(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{}-{}", prefix, nanos)
    }

    #[test]
    fn test_bad_entry_path() {
        let args = rm_attachment_cmd().try_get_matches_from(vec!["rm", "local/bucket"]);
        assert_eq!(
            args.unwrap_err().to_string(),
            "error: invalid value 'local/bucket' for '<ENTRY_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }

    #[test]
    fn test_rm_requires_target() {
        let args = rm_attachment_cmd().try_get_matches_from(vec!["rm", "local/bucket/entry-1"]);
        assert!(args
            .unwrap_err()
            .to_string()
            .contains("required arguments were not provided"));
    }

    #[test]
    fn test_rm_all_conflicts_with_keys() {
        let args = rm_attachment_cmd().try_get_matches_from(vec![
            "rm",
            "local/bucket/entry-1",
            "--all",
            "schema",
        ]);
        assert!(args
            .unwrap_err()
            .to_string()
            .contains("cannot be used with"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_rm_selected_attachments(context: CliContext) {
        let bucket_name = unique_bucket_name("test-attachment-rm-key");
        let client = build_client(&context, "local").await.unwrap();
        let bucket = client.create_bucket(&bucket_name).send().await.unwrap();
        bucket
            .write_attachments(
                "entry-1",
                HashMap::from([
                    ("schema".to_string(), json!({"type":"object"})),
                    ("prompt".to_string(), json!({"role":"system"})),
                ]),
            )
            .await
            .unwrap();

        let args = rm_attachment_cmd()
            .try_get_matches_from(vec![
                "rm",
                &format!("local/{}/entry-1", bucket_name),
                "prompt",
            ])
            .unwrap();
        rm_attachment(&context, &args).await.unwrap();

        let attachments = read_attachments_or_empty(&bucket, "entry-1").await.unwrap();
        assert_eq!(attachments.len(), 1);
        assert!(attachments.contains_key("schema"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_rm_all_attachments(context: CliContext) {
        let bucket_name = unique_bucket_name("test-attachment-rm-all");
        let client = build_client(&context, "local").await.unwrap();
        let bucket = client.create_bucket(&bucket_name).send().await.unwrap();
        bucket
            .write_attachments(
                "entry-1",
                HashMap::from([("schema".to_string(), json!({"type":"object"}))]),
            )
            .await
            .unwrap();

        let args = rm_attachment_cmd()
            .try_get_matches_from(vec![
                "rm",
                &format!("local/{}/entry-1", bucket_name),
                "--all",
            ])
            .unwrap();
        rm_attachment(&context, &args).await.unwrap();

        let attachments = read_attachments_or_empty(&bucket, "entry-1").await.unwrap();
        assert!(attachments.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn test_rm_missing_entry(context: CliContext) {
        let bucket_name = unique_bucket_name("test-attachment-rm-missing");
        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket_name).send().await.unwrap();

        let args = rm_attachment_cmd()
            .try_get_matches_from(vec![
                "rm",
                &format!("local/{}/entry-1", bucket_name),
                "--all",
            ])
            .unwrap();
        rm_attachment(&context, &args).await.unwrap();
    }
}
