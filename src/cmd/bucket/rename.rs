// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::cmd::bucket::{create_update_bucket_args, parse_bucket_settings};

use crate::context::CliContext;

use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::ReductClient;

pub(super) fn rename_bucket_cmd() -> Command {
    let cmd = Command::new("rename").about("Rename a bucket");
    cmd.arg(
        Arg::new("BUCKET_PATH")
            .help(crate::cmd::RESOURCE_PATH_HELP)
            .value_parser(crate::parse::ResourcePathParser::new())
            .required(true),
    )
    .arg(
        Arg::new("NEW_NAME")
            .help("New name for the bucket")
            .required(true),
    )
}

pub(super) async fn rename_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, bucket_name) = args.get_one::<(String, String)>("BUCKET_PATH").unwrap();
    let new_name = args.get_one::<String>("NEW_NAME").unwrap();

    let client: ReductClient = build_client(ctx, alias_or_url).await?;
    client
        .get_bucket(bucket_name)
        .await?
        .rename(new_name)
        .await?;

    output!(ctx, "Bucket '{}' renamed to '{}'", bucket_name, new_name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, context};
    use reduct_rs::QuotaType;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_rename_bucket(context: CliContext, #[future] bucket: String) {
        let bucket_name = bucket.await;
        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket_name).send().await.unwrap();

        let args = rename_bucket_cmd().get_matches_from(vec![
            "rename",
            format!("local/{}", bucket_name).as_str(),
            "new_bucket",
        ]);

        assert_eq!(
            rename_bucket(&context, &args).await.unwrap(),
            (),
            "Rename bucket succeeded"
        );

        let bucket = client.get_bucket("new_bucket").await.unwrap();
        bucket.remove().await.unwrap();

        assert!(
            client.get_bucket(&bucket_name).await.is_err(),
            "Old bucket does not exist"
        );
        assert!(
            context
                .stdout()
                .history()
                .contains(&"Bucket 'test_bucket' renamed to 'new_bucket'".to_string()),
            "Output contains message"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_bucket_invalid_path() {
        let args = rename_bucket_cmd().try_get_matches_from(vec!["update", "local"]);
        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'local' for '<BUCKET_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }
}
