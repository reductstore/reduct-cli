use crate::context::CliContext;

use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::ReductClient;

pub(super) fn rename_bucket_cmd() -> Command {
    let cmd = Command::new("rename").about("Rename a bucket or an entry");
    cmd.arg(
        Arg::new("BUCKET_PATH")
            .help(crate::cmd::RESOURCE_PATH_HELP)
            .value_parser(crate::parse::ResourcePathParser::new())
            .required(true),
    )
    .arg(
        Arg::new("NEW_NAME")
            .help("New name for the bucket or entry")
            .required(true),
    )
    .arg(
        Arg::new("only-entry")
            .long("only-entry")
            .short('e')
            .value_name("ENTRY_NAME")
            .help("Rename an entry instead of a bucket")
            .required(false),
    )
}

pub(super) async fn rename_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, bucket_name) = args.get_one::<(String, String)>("BUCKET_PATH").unwrap();
    let new_name = args.get_one::<String>("NEW_NAME").unwrap();
    let entry_name = args.get_one::<String>("only-entry");

    let client: ReductClient = build_client(ctx, alias_or_url).await?;
    if let Some(entry_name) = entry_name {
        let bucket = client.get_bucket(bucket_name).await?;
        bucket.rename_entry(entry_name, new_name).await?;
        output!(ctx, "Entry '{}' renamed to '{}'", entry_name, new_name);
    } else {
        client
            .get_bucket(bucket_name)
            .await?
            .rename(new_name)
            .await?;

        output!(ctx, "Bucket '{}' renamed to '{}'", bucket_name, new_name);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, context};
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
    async fn test_rename_entry(context: CliContext, #[future] bucket: String) {
        let bucket_name = bucket.await;
        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket_name).send().await.unwrap();

        client
            .get_bucket(&bucket_name)
            .await
            .unwrap()
            .write_record("test")
            .timestamp_us(0)
            .data("test")
            .send()
            .await
            .unwrap();

        let args = rename_bucket_cmd().get_matches_from(vec![
            "rename",
            format!("local/{}", bucket_name).as_str(),
            "--only-entry",
            "test",
            "new_test",
        ]);

        assert_eq!(
            rename_bucket(&context, &args).await.unwrap(),
            (),
            "Rename entry succeeded"
        );

        let bucket = client.get_bucket(&bucket_name).await.unwrap();
        assert!(
            bucket
                .read_record("new_test")
                .timestamp_us(0)
                .send()
                .await
                .is_ok(),
            "Entry exists with new name"
        );
        assert!(
            bucket
                .read_record("test")
                .timestamp_us(0)
                .send()
                .await
                .is_err(),
            "Old entry does not exist"
        );
        assert!(
            context
                .stdout()
                .history()
                .contains(&"Entry 'test' renamed to 'new_test'".to_string()),
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
