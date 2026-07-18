use clap::{Arg, Command};

use crate::{
    context::CliContext,
    io::{reduct::build_client, std::output},
    parse::{Resource, ResourcePathParser},
};

pub(crate) fn write_record_cmd() -> Command {
    Command::new("write")
        .about("Write single record to a bucket")
        .arg(
            Arg::new("ENTRY_PATH")
                .help("Full path the entry to write to")
                .value_parser(ResourcePathParser::new().allow_alias())
                .required(true),
        )
        .arg(
            Arg::new("payload")
                .long("string")
                .short('s')
                .help("inline payload string.")
                .required(true)
                .value_name("PAYLOAD"),
        )
}

pub(crate) async fn write_handler(ctx: &CliContext, args: &clap::ArgMatches) -> anyhow::Result<()> {
    let entry_path = args.get_one::<Resource>("ENTRY_PATH").unwrap().clone();
    let (alias_or_url, bucket_name, entry_name) = entry_path.triple()?;

    let entry_name = entry_name
        .ok_or_else(|| anyhow::anyhow!("ENTRY_PATH must be alias/bucket/path/to/entry"))?;

    let payload = args.get_one::<String>("payload").unwrap().clone();

    let client = build_client(ctx, &alias_or_url).await?;

    let bucket = client.get_bucket(&bucket_name).await?;
    bucket
        .write_record(&entry_name)
        .data(payload)
        .content_type("application/text")
        .send()
        .await?;

    output!(ctx, "Record written to '{}/{}'", bucket_name, entry_name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::context::tests::context;
    use reduct_rs::Bucket;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_write_string_record(
        context: CliContext,
        #[future] bucket: Bucket,
        entry_path: String,
        payload: String,
    ) -> anyhow::Result<()> {
        let bucket = bucket.await;

        // Prepare args
        let args = write_record_cmd().get_matches_from(vec![
            "write",
            format!("local/{}/{}", bucket.name(), entry_path).as_str(),
            "--string",
            payload.as_str(),
        ]);

        write_handler(&context, &args).await?;

        let record = bucket.read_record(&entry_path).send().await?;

        assert_eq!(record.bytes().await?, payload.as_bytes());
        assert_eq!(
            context.stdout().history(),
            vec![format!(
                "Record written to '{}/{}'",
                bucket.name(),
                entry_path
            )]
        );

        Ok(())
    }

    #[fixture]
    fn entry_path() -> String {
        "test-entry".to_string()
    }

    #[fixture]
    fn payload() -> String {
        "test-payload".to_string()
    }

    #[fixture]
    async fn bucket(context: CliContext) -> Bucket {
        let bucket_name = unique_name("test-bucket");
        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket_name).send().await.unwrap()
    }

    fn unique_name(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{}-{}", prefix, nanos)
    }
}
