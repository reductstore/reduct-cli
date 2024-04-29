// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::context::CliContext;

use crate::cmd::RESOURCE_PATH_HELP;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::{fetch_and_filter_entries, ResourcePathParser};
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::ReductClient;

pub(super) fn rm_bucket_cmd() -> Command {
    Command::new("rm")
        .about("Remove a bucket")
        .arg(
            Arg::new("BUCKET_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("only-entries")
                .long("only-entries")
                .short('e')
                .value_name("ENTRY_NAME")
                .help("Remove only the specified entry instead of the whole bucket")
                .num_args(1..)
                .required(false),
        )
        .arg(
            Arg::new("yes")
                .long("yes")
                .short('y')
                .action(SetTrue)
                .help("Do not ask for confirmation")
                .required(false),
        )
}

pub(super) async fn rm_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, bucket_name) = args.get_one::<(String, String)>("BUCKET_PATH").unwrap();
    let only_entries = args
        .get_many::<String>("only-entries")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    if !only_entries.is_empty() {
        remove_entries(ctx, args, alias_or_url, bucket_name, only_entries).await?;
    } else {
        remove_entire_bucket(ctx, args, alias_or_url, bucket_name).await?;
    }
    Ok(())
}

async fn remove_entire_bucket(
    ctx: &CliContext,
    args: &ArgMatches,
    alias_or_url: &String,
    bucket_name: &String,
) -> anyhow::Result<()> {
    let yes = args.get_flag("yes");
    let confirm = if !yes {
        let confirm = dialoguer::Confirm::new()
            .default(false)
            .with_prompt(format!(
                "Are you sure you want to delete bucket '{}'?",
                bucket_name
            ))
            .interact()?;
        confirm
    } else {
        true
    };

    if confirm {
        let client: ReductClient = build_client(ctx, alias_or_url).await?;
        client.get_bucket(bucket_name).await?.remove().await?;

        output!(ctx, "Bucket '{}' deleted", bucket_name);
    } else {
        output!(ctx, "Bucket '{}' not deleted", bucket_name);
    }
    Ok(())
}

async fn remove_entries(
    ctx: &CliContext,
    args: &ArgMatches,
    alias_or_url: &String,
    bucket_name: &String,
    only_entries: Vec<String>,
) -> anyhow::Result<()> {
    let client: ReductClient = build_client(ctx, alias_or_url).await?;
    let bucket = client.get_bucket(bucket_name).await?;

    let entries = fetch_and_filter_entries(&bucket, &only_entries)
        .await?
        .iter()
        .map(|entry| entry.name.clone())
        .collect::<Vec<String>>();
    if entries.is_empty() {
        output!(ctx, "No entries found to delete");
        return Ok(());
    }

    let yes = args.get_flag("yes");
    let confirm = if !yes {
        let confirm = dialoguer::Confirm::new()
            .default(false)
            .with_prompt(format!(
                "Are you sure you want to delete {:?} entries from bucket '{}'?",
                entries, bucket_name,
            ))
            .interact()?;
        confirm
    } else {
        true
    };

    if confirm {
        for entry in &entries {
            bucket.remove_entry(entry).await?;
        }

        output!(ctx, "Entries {:?} deleted", entries);
    } else {
        output!(ctx, "Entries {:?} not deleted", entries);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, context};
    use bytes::Bytes;
    use reduct_rs::ErrorCode;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_rm_bucket(context: CliContext, #[future] bucket: String) {
        let bucket_name = bucket.await;
        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket_name).send().await.unwrap();

        let args = rm_bucket_cmd().get_matches_from(vec![
            "rm",
            format!("local/{}", bucket_name).as_str(),
            "--yes",
        ]);

        assert_eq!(rm_bucket(&context, &args).await.unwrap(), ());
        assert_eq!(
            client
                .get_bucket(&bucket_name)
                .await
                .err()
                .unwrap()
                .status(),
            ErrorCode::NotFound
        );
        assert_eq!(
            context.stdout().history(),
            vec!["Bucket 'test_bucket' deleted"]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rm_bucket_only_entries(context: CliContext, #[future] bucket: String) {
        let bucket_name = bucket.await;
        let client = build_client(&context, "local").await.unwrap();
        let bucket = client.create_bucket(&bucket_name).send().await.unwrap();

        bucket
            .write_record("test")
            .data(Bytes::from_static(b"test"))
            .send()
            .await
            .unwrap();

        let args = rm_bucket_cmd().get_matches_from(vec![
            "rm",
            format!("local/{}", bucket_name).as_str(),
            "--yes",
            "--only-entries",
            "test",
        ]);

        assert_eq!(rm_bucket(&context, &args).await.unwrap(), ());
        assert_eq!(
            client
                .get_bucket(&bucket_name)
                .await
                .unwrap()
                .entries()
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            context.stdout().history(),
            vec!["Entries [\"test\"] deleted"]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rm_bucket_invalid_path() {
        let args = rm_bucket_cmd().try_get_matches_from(vec!["rm", "local"]);

        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'local' for '<BUCKET_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }
}
