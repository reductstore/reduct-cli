// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::attachment::helpers::{entry_path_arg, read_attachments_or_empty, EntryPath};
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::{Arg, ArgMatches, Command};

pub(super) fn read_attachment_cmd() -> Command {
    Command::new("read")
        .about("Read attachments of an entry")
        .arg(entry_path_arg("ENTRY_PATH"))
        .arg(
            Arg::new("KEY")
                .help("Attachment keys to read")
                .value_name("KEY")
                .num_args(0..)
                .required(false),
        )
}

pub(super) async fn read_attachment(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let path = args.get_one::<EntryPath>("ENTRY_PATH").unwrap();
    let selected_keys = args
        .get_many::<String>("KEY")
        .map(|values| values.map(|v| v.to_string()).collect::<Vec<String>>())
        .unwrap_or_default();

    let client = build_client(ctx, &path.alias_or_url).await?;
    let bucket = client.get_bucket(&path.bucket).await?;
    let attachments = read_attachments_or_empty(&bucket, &path.entry).await?;

    if selected_keys.is_empty() {
        let mut keys = attachments.keys().cloned().collect::<Vec<String>>();
        keys.sort();
        for key in keys {
            let value = attachments.get(&key).unwrap();
            output!(ctx, "{}: {}", key, serde_json::to_string(value)?);
        }
        return Ok(());
    }

    for key in selected_keys {
        let value = attachments.get(&key).ok_or_else(|| {
            anyhow::anyhow!(
                "Attachment '{}' not found in '{}/{}/{}'",
                key,
                path.alias_or_url,
                path.bucket,
                path.entry
            )
        })?;
        output!(ctx, "{}: {}", key, serde_json::to_string(value)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::attachment::helpers::test_utils::create_bucket;
    use crate::context::tests::context;
    use rstest::rstest;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_bad_entry_path() {
        let args = read_attachment_cmd().try_get_matches_from(vec!["read", "local/bucket"]);
        assert_eq!(
            args.unwrap_err().to_string(),
            "error: invalid value 'local/bucket' for '<ENTRY_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_all_attachments(context: CliContext) {
        let (bucket_name, bucket) = create_bucket(&context, "test-attachment-read-all")
            .await
            .unwrap();
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

        let args = read_attachment_cmd()
            .try_get_matches_from(vec!["read", &format!("local/{}/entry-1", bucket_name)])
            .unwrap();
        read_attachment(&context, &args).await.unwrap();

        assert_eq!(
            context.stdout().history(),
            vec![
                "prompt: {\"role\":\"system\"}",
                "schema: {\"type\":\"object\"}"
            ]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_selected_attachments(context: CliContext) {
        let (bucket_name, bucket) = create_bucket(&context, "test-attachment-read-key")
            .await
            .unwrap();
        bucket
            .write_attachments(
                "entry-1",
                HashMap::from([("schema".to_string(), json!({"type":"object"}))]),
            )
            .await
            .unwrap();

        let args = read_attachment_cmd()
            .try_get_matches_from(vec![
                "read",
                &format!("local/{}/entry-1", bucket_name),
                "schema",
            ])
            .unwrap();
        read_attachment(&context, &args).await.unwrap();
        assert_eq!(
            context.stdout().history(),
            vec!["schema: {\"type\":\"object\"}"]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_missing_attachment(context: CliContext) {
        let (bucket_name, _) = create_bucket(&context, "test-attachment-read-missing")
            .await
            .unwrap();

        let args = read_attachment_cmd()
            .try_get_matches_from(vec![
                "read",
                &format!("local/{}/entry-1", bucket_name),
                "key",
            ])
            .unwrap();
        let err = read_attachment(&context, &args).await.err().unwrap();
        assert!(err
            .to_string()
            .contains("Attachment 'key' not found in 'local/"));
    }
}
