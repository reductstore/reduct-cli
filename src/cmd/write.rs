use bytes::Bytes;
use bytesize::ByteSize;
use clap::{Arg, Command};
use futures_util::stream::Stream;
use indicatif::{ProgressBar, ProgressStyle};
use reduct_rs::Labels;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::io::ReaderStream;

use crate::{
    context::CliContext,
    io::{reduct::build_client, std::output},
    parse::{Resource, ResourcePathParser},
};

/// A wrapper stream that tracks upload progress
struct ProgressStream<S> {
    inner: S,
    progress_bar: ProgressBar,
}

impl<S> ProgressStream<S> {
    fn new(inner: S, progress_bar: ProgressBar) -> Self {
        Self {
            inner,
            progress_bar,
        }
    }
}

impl<S, E> Stream for ProgressStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.progress_bar.inc(chunk.len() as u64);
                Poll::Ready(Some(Ok(chunk)))
            }
            other => other,
        }
    }
}

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
                .value_name("PAYLOAD")
                .conflicts_with("path"),
        )
        .arg(
            Arg::new("path")
                .long("file")
                .short('f')
                .help("payload file path.")
                .required(false)
                .value_name("PATH")
                .conflicts_with("payload"),
        )
}

pub(crate) async fn write_handler(ctx: &CliContext, args: &clap::ArgMatches) -> anyhow::Result<()> {
    let entry_path = args.get_one::<Resource>("ENTRY_PATH").unwrap().clone();
    let (alias_or_url, bucket_name, entry_name) = entry_path.triple()?;

    let entry_name = entry_name
        .ok_or_else(|| anyhow::anyhow!("ENTRY_PATH must be alias/bucket/path/to/entry"))?;

    let client = build_client(ctx, &alias_or_url).await?;

    let bucket = client.get_bucket(&bucket_name).await?;

    if let Some(path) = args.get_one::<String>("path") {
        let reader_stream = ReaderStream::new(tokio::fs::File::open(path).await?);
        let content_length = tokio::fs::metadata(path).await?.len();

        // Create a progress bar for file upload
        let progress_bar = ProgressBar::new(content_length);
        progress_bar.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:40.green/gray} {bytes}/{total_bytes} ({bytes_per_sec}) {msg}",
            )?
            .tick_strings(&["▁", "▃", "▄", "▅", "▆", "▇"]),
        );
        progress_bar.set_message(format!("Uploading to {}/{}", bucket_name, entry_name));

        // Wrap the stream with progress tracking
        let progress_stream = ProgressStream::new(reader_stream, progress_bar.clone());

        bucket
            .write_record(&entry_name)
            // .content_type("application/text")
            .labels(Labels::from([("name".to_string(), "malhar".to_string())]))
            .stream(progress_stream)
            .content_length(content_length)
            .send()
            .await?;

        progress_bar.finish_with_message(format!(
            "Uploaded {} to {}/{}",
            ByteSize::b(content_length),
            bucket_name,
            entry_name
        ));
    } else {
        let data = args
            .get_one::<String>("payload")
            .unwrap()
            .as_bytes()
            .to_vec();
        // ReaderStream::new(bytes.as_slice())
        bucket
            .write_record(&entry_name)
            .data(data)
            .content_type("application/text")
            .labels(Labels::from([("is_file".to_string(), "false".to_string())]))
            .send()
            .await?;
    };

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
