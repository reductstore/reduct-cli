// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::attachment::helpers::{entry_path_arg, read_attachments_or_empty, EntryPath};
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::{ArgMatches, Command};

pub(super) fn ls_attachment_cmd() -> Command {
    Command::new("ls")
        .about("List attachment keys of an entry")
        .arg(entry_path_arg("ENTRY_PATH"))
}

pub(super) async fn ls_attachment(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let path = args.get_one::<EntryPath>("ENTRY_PATH").unwrap();

    let client = build_client(ctx, &path.alias_or_url).await?;
    let bucket = client.get_bucket(&path.bucket).await?;
    let attachments = read_attachments_or_empty(&bucket, &path.entry).await?;

    let mut keys = attachments.keys().cloned().collect::<Vec<String>>();
    keys.sort();
    for key in keys {
        output!(ctx, "{}", key);
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
        let args = ls_attachment_cmd().try_get_matches_from(vec!["ls", "local/bucket"]);
        assert_eq!(
            args.unwrap_err().to_string(),
            "error: invalid value 'local/bucket' for '<ENTRY_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_ls_attachments(context: CliContext) {
        let (bucket_name, bucket) = create_bucket(&context, "test-attachment-ls").await.unwrap();
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

        let args = ls_attachment_cmd()
            .try_get_matches_from(vec!["ls", &format!("local/{}/entry-1", bucket_name)])
            .unwrap();
        ls_attachment(&context, &args).await.unwrap();

        assert_eq!(context.stdout().history(), vec!["prompt", "schema"]);
    }

    #[rstest]
    #[tokio::test]
    async fn test_ls_attachments_empty(context: CliContext) {
        let (bucket_name, _) = create_bucket(&context, "test-attachment-ls-empty")
            .await
            .unwrap();

        let args = ls_attachment_cmd()
            .try_get_matches_from(vec!["ls", &format!("local/{}/entry-1", bucket_name)])
            .unwrap();
        ls_attachment(&context, &args).await.unwrap();

        assert!(context.stdout().history().is_empty());
    }
}
