// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::attachment::helpers::{entry_path_arg, EntryPath};
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::{Arg, ArgMatches, Command};
use serde_json::Value;
use std::collections::HashMap;

pub(super) fn write_attachment_cmd() -> Command {
    Command::new("write")
        .about("Write an attachment to an entry")
        .arg(entry_path_arg("ENTRY_PATH"))
        .arg(
            Arg::new("KEY")
                .help("Attachment key")
                .required(true)
                .value_name("KEY"),
        )
        .arg(
            Arg::new("VALUE")
                .help("Attachment value in JSON format")
                .required(true)
                .value_name("JSON"),
        )
}

pub(super) async fn write_attachment(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let path = args.get_one::<EntryPath>("ENTRY_PATH").unwrap();
    let key = args.get_one::<String>("KEY").unwrap();
    let value = args.get_one::<String>("VALUE").unwrap();
    let json: Value = serde_json::from_str(value).map_err(|err| {
        anyhow::anyhow!(
            "Failed to parse attachment value for key '{}': {}",
            key,
            err
        )
    })?;

    let mut attachments = HashMap::new();
    attachments.insert(key.clone(), json);

    let client = build_client(ctx, &path.alias_or_url).await?;
    let bucket = client.get_bucket(&path.bucket).await?;
    bucket.write_attachments(&path.entry, attachments).await?;

    output!(
        ctx,
        "Attachment '{}' written to '{}/{}/{}'",
        key,
        path.alias_or_url,
        path.bucket,
        path.entry
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::attachment::helpers::test_utils::{create_bucket, remove_bucket};
    use crate::context::tests::context;
    use rstest::rstest;
    use serde_json::json;

    #[test]
    fn test_bad_entry_path() {
        let args = write_attachment_cmd().try_get_matches_from(vec!["write", "local/bucket"]);
        assert_eq!(
            args.unwrap_err().to_string(),
            "error: invalid value 'local/bucket' for '<ENTRY_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }

    #[test]
    fn test_url_with_base_path() {
        let args = write_attachment_cmd()
            .try_get_matches_from(vec![
                "write",
                "https://reductstore@play.reduct.store/replica/datasets/entry-1",
                "schema",
                "{\"type\":\"object\"}",
            ])
            .unwrap();
        let path = args.get_one::<EntryPath>("ENTRY_PATH").unwrap();
        assert_eq!(
            *path,
            EntryPath {
                alias_or_url: "https://reductstore@play.reduct.store/replica".to_string(),
                bucket: "datasets".to_string(),
                entry: "entry-1".to_string(),
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_attachment(context: CliContext) {
        let (bucket_name, bucket) = create_bucket(&context, "test-attachment-write")
            .await
            .unwrap();
        let args = write_attachment_cmd()
            .try_get_matches_from(vec![
                "write",
                &format!("local/{}/entry-1", bucket_name),
                "schema",
                "{\"type\":\"object\"}",
            ])
            .unwrap();

        write_attachment(&context, &args).await.unwrap();

        let attachments = bucket.read_attachments("entry-1").await.unwrap();
        remove_bucket(&context, &bucket_name).await.unwrap();

        assert_eq!(attachments.get("schema"), Some(&json!({"type":"object"})));
        assert_eq!(
            context.stdout().history()[0],
            format!(
                "Attachment 'schema' written to 'local/{}/entry-1'",
                bucket_name
            )
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_invalid_json(context: CliContext) {
        let args = write_attachment_cmd()
            .try_get_matches_from(vec!["write", "local/bucket/entry-1", "schema", "{"])
            .unwrap();

        let err = write_attachment(&context, &args).await.err().unwrap();
        assert!(err
            .to_string()
            .contains("Failed to parse attachment value for key 'schema'"));
    }
}
