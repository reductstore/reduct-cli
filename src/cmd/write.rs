use bytes::Bytes;
use bytesize::ByteSize;
use clap::{Arg, Command};
use futures_util::stream::Stream;
use indicatif::{ProgressBar, ProgressStyle};
use reduct_rs::WriteRecordBuilder;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;
use tokio_util::io::ReaderStream;

use crate::parse::{parse_labels, parse_time};
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
                .required(true)
                .value_name("PATH")
                .conflicts_with("payload"),
        )
        .arg(
            Arg::new("content-type")
                .long("content-type")
                .short('C')
                .help("record content type.")
                .required(false)
                .value_name("MIME"),
        )
        .arg(
            Arg::new("timestamp")
                .long("timestamp")
                .short('t')
                .help("RFC 3339 / ISO timestamp or Unix timestamp in microseconds.")
                .required(false)
                .value_name("TIME"),
        )
        .arg(
            Arg::new("quiet")
                .long("quiet")
                .short('q')
                .help("suppress successful output.")
                .required(false)
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("labels")
                .long("labels")
                .short('l')
                .help("labels as key=value,key=value or a JSON object.")
                .required(false)
                .value_name("LABELS"),
        )
}

pub(crate) async fn write_handler(ctx: &CliContext, args: &clap::ArgMatches) -> anyhow::Result<()> {
    let entry_path = args.get_one::<Resource>("ENTRY_PATH").unwrap().clone();
    let (alias_or_url, bucket_name, entry_name) = entry_path.triple()?;

    let entry_name = entry_name
        .ok_or_else(|| anyhow::anyhow!("ENTRY_PATH must be alias/bucket/path/to/entry"))?;

    let content_type = args.get_one::<String>("content-type");

    let client = build_client(ctx, &alias_or_url).await?;
    let bucket = client.get_bucket(&bucket_name).await?;
    let mut write_record_builder = bucket.write_record(&entry_name);

    // Parse labels if provided
    if let Some(labels_str) = args.get_one::<String>("labels") {
        let labels = parse_labels(labels_str)?;
        write_record_builder = write_record_builder.labels(labels);
    }

    // Parse timestamp from argument or use current time
    let timestamp_us = if let Some(timestamp_str) = args.get_one::<String>("timestamp") {
        parse_time(Some(timestamp_str))
            .map_err(|e| anyhow::anyhow!("Invalid timestamp: {}", e))?
            .ok_or_else(|| anyhow::anyhow!("Timestamp parsing returned None"))?
    } else {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_micros() as u64
    };

    write_record_builder = write_record_builder.timestamp_us(timestamp_us);

    if let Some(path) = args.get_one::<String>("path") {
        // Check file path validity
        if !tokio::fs::metadata(path).await?.is_file() {
            return Err(anyhow::anyhow!("Path '{}' is invalid or not a file", path));
        }

        // Guess content type from file extension if not provided
        let content_type = if let Some(ct) = content_type {
            ct.to_string()
        } else {
            let extension = std::path::Path::new(path)
                .extension()
                .and_then(|ext| ext.to_str())
                .unwrap_or("");
            mime_guess::from_ext(extension)
                .first_or_octet_stream()
                .to_string()
        };

        let write_record_builder = write_record_builder.content_type(content_type);
        write_file_record(ctx, &bucket_name, &entry_name, path, write_record_builder).await?;
    } else {
        // If no path is provided, use the string payload
        let content_type = content_type
            .map(|ct| ct.to_string())
            .unwrap_or_else(|| "text/plain".to_string());

        let write_record_builder = write_record_builder.content_type(content_type);

        let data = args
            .get_one::<String>("payload")
            .unwrap()
            .as_bytes()
            .to_vec();

        write_record_builder.data(data).send().await?;
    };

    if !args.get_flag("quiet") {
        output!(ctx, "Record written to '{}/{}'", bucket_name, entry_name);
    }

    Ok(())
}

async fn write_file_record(
    _ctx: &CliContext,
    bucket_name: &str,
    entry_name: &str,
    file_path: &str,
    // args: &clap::ArgMatches,
    write_record_builder: WriteRecordBuilder,
) -> anyhow::Result<()> {
    let file = tokio::fs::File::open(file_path).await?;
    let content_length = file.metadata().await?.len();

    if content_length == 0 {
        return Err(anyhow::anyhow!("File is empty"));
    }

    let reader_stream = ReaderStream::new(file);

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

    write_record_builder
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

    #[rstest]
    #[tokio::test]
    async fn test_write_file_with_mime_guess(
        context: CliContext,
        #[future] bucket: Bucket,
    ) -> anyhow::Result<()> {
        let bucket = bucket.await;
        let temp_dir = tempfile::tempdir()?;
        let file_path = temp_dir.path().join("test.json");
        tokio::fs::write(&file_path, b"{\"test\": \"data\"}").await?;

        // Write without explicit content-type, should guess from extension
        let args = write_record_cmd().get_matches_from(vec![
            "write",
            format!("local/{}/test-entry", bucket.name()).as_str(),
            "--file",
            file_path.to_str().unwrap(),
        ]);

        write_handler(&context, &args).await?;

        let record = bucket.read_record("test-entry").send().await?;
        assert_eq!(record.content_type(), "application/json");
        assert_eq!(record.bytes().await?.as_ref(), b"{\"test\": \"data\"}");

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
