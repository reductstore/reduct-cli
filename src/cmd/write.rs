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

    mod string_record_tests {

        use super::*;

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
        async fn test_write_string_record_with_content_type(
            context: CliContext,
            #[future] bucket: Bucket,
            entry_path: String,
            payload: String,
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;

            let content_type = "text/html";

            // Prepare args
            let args = write_record_cmd().get_matches_from(vec![
                "write",
                format!("local/{}/{}", bucket.name(), entry_path).as_str(),
                "--string",
                payload.as_str(),
                "--content-type",
                content_type,
            ]);

            write_handler(&context, &args).await?;

            let record = bucket.read_record(&entry_path).send().await?;

            assert_eq!(record.content_type(), content_type);

            Ok(())
        }
    }

    mod file_record_tests {

        use super::*;

        /*
            Test content-type handling for file uploads.
            Tests: no extension (octet-stream), MIME guessing, and explicit content-type.
        */
        #[rstest]
        #[case("testfile", b"Hello World", None, "application/octet-stream")]
        #[case("test.json", b"{\"test\": \"data\"}", None, "application/json")]
        #[case(
            "test.txt",
            b"hello world",
            Some("application/json"),
            "application/json"
        )]
        #[tokio::test]
        async fn test_write_file_content_type(
            context: CliContext,
            #[future] bucket: Bucket,
            #[case] filename: &str,
            #[case] content: &[u8],
            #[case] explicit_content_type: Option<&str>,
            #[case] expected_content_type: &str,
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;
            let temp_dir = tempfile::tempdir()?;
            let file_path = temp_dir.path().join(filename);
            tokio::fs::write(&file_path, content).await?;

            let entry_path = format!("local/{}/test-entry", bucket.name());
            let mut args_vec = vec![
                "write",
                entry_path.as_str(),
                "--file",
                file_path.to_str().unwrap(),
            ];

            // Add content-type if explicitly provided
            if let Some(ct) = explicit_content_type {
                args_vec.push("--content-type");
                args_vec.push(ct);
            }

            let args = write_record_cmd().get_matches_from(args_vec);
            write_handler(&context, &args).await?;

            let record = bucket.read_record("test-entry").send().await?;
            assert_eq!(record.content_type(), expected_content_type);

            Ok(())
        }

        /*
            Test that writing an empty file returns an error.
        */
        #[rstest]
        #[tokio::test]
        async fn test_write_empty_file(
            context: CliContext,
            #[future] bucket: Bucket,
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;
            let temp_dir = tempfile::tempdir()?;
            let file_path = temp_dir.path().join("empty.txt");
            tokio::fs::write(&file_path, b"").await?;

            let args = write_record_cmd().get_matches_from(vec![
                "write",
                format!("local/{}/test-entry", bucket.name()).as_str(),
                "-f",
                file_path.to_str().unwrap(),
            ]);

            let result = write_handler(&context, &args).await;

            assert!(result.is_err());
            let error_msg = result.unwrap_err().to_string();
            assert!(
                error_msg.contains("File is empty"),
                "Expected error about empty file, got: {}",
                error_msg
            );

            Ok(())
        }

        /*
            Test error conditions for invalid file paths.
            Tests: file not found, directory instead of file.
        */
        #[rstest]
        #[case("not_exist.json", "No such file or directory")]
        #[case("/tmp/somedir", "No such file or directory")]
        #[tokio::test]
        async fn test_write_file_path_errors(
            context: CliContext,
            #[future] bucket: Bucket,
            #[case] file_path: &str,
            #[case] expected_error: &str,
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;

            let entry_path = format!("local/{}/test-entry", bucket.name());
            let args = write_record_cmd().get_matches_from(vec![
                "write",
                entry_path.as_str(),
                "-f",
                file_path,
            ]);

            let result = write_handler(&context, &args).await;

            assert!(result.is_err());
            let error_msg = result.unwrap_err().to_string();
            assert!(
                error_msg.contains(expected_error),
                "Expected error containing '{}', got: {}",
                expected_error,
                error_msg
            );

            Ok(())
        }
    }

    mod timestamp_tests {
        use super::*;
        use crate::parse::parse_time;

        /*
            Test writing records with valid timestamp formats.
            Tests RFC 3339 UTC, RFC 3339 with offset, and Unix microseconds.
        */
        #[rstest]
        #[case("2026-01-01T01:00:00Z", "RFC 3339 UTC")]
        #[case("2025-06-15T14:30:00+05:30", "RFC 3339 with offset")]
        #[case("1672531200000000", "Unix microseconds")]
        #[tokio::test]
        async fn test_write_file_with_valid_timestamps(
            context: CliContext,
            #[future] bucket: Bucket,
            #[case] timestamp_str: &str,
            #[case] _description: &str,
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;

            let temp_dir = tempfile::tempdir()?;
            let file_path = temp_dir.path().join("testfile");
            tokio::fs::write(&file_path, b"test data").await?;

            let args = write_record_cmd().get_matches_from(vec![
                "write",
                format!("local/{}/test-entry", bucket.name()).as_str(),
                "-f",
                file_path.to_str().unwrap(),
                "-t",
                timestamp_str,
            ]);

            write_handler(&context, &args).await?;

            let expected_micros =
                parse_time(Some(&timestamp_str.to_string()))?.expect("timestamp should parse");

            let record = bucket.read_record("test-entry").send().await?;
            assert_eq!(record.timestamp_us(), expected_micros);

            Ok(())
        }

        /*
            Test writing string records with a timestamp.
        */
        #[rstest]
        #[tokio::test]
        async fn test_write_string_with_timestamp(
            context: CliContext,
            #[future] bucket: Bucket,
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;

            let timestamp_str = "2025-12-25T12:00:00Z";
            let args = write_record_cmd().get_matches_from(vec![
                "write",
                format!("local/{}/test-entry", bucket.name()).as_str(),
                "-s",
                "test payload",
                "-t",
                timestamp_str,
            ]);

            write_handler(&context, &args).await?;

            let expected_micros =
                parse_time(Some(&timestamp_str.to_string()))?.expect("timestamp should parse");

            let record = bucket.read_record("test-entry").send().await?;
            assert_eq!(record.timestamp_us(), expected_micros);
            assert_eq!(record.bytes().await?.as_ref(), b"test payload");

            Ok(())
        }

        /*
            Test that writing without a timestamp uses the current time.
        */
        #[rstest]
        #[tokio::test]
        async fn test_write_without_timestamp_uses_current_time(
            context: CliContext,
            #[future] bucket: Bucket,
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;

            let before_us = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;

            let args = write_record_cmd().get_matches_from(vec![
                "write",
                format!("local/{}/test-entry", bucket.name()).as_str(),
                "-s",
                "test payload",
            ]);

            write_handler(&context, &args).await?;

            let after_us = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;

            let record = bucket.read_record("test-entry").send().await?;
            let timestamp = record.timestamp_us();

            // Timestamp should be between before and after
            assert!(
                timestamp >= before_us && timestamp <= after_us,
                "Timestamp {} should be between {} and {}",
                timestamp,
                before_us,
                after_us
            );

            Ok(())
        }

        /*
            Test that invalid timestamp formats are rejected.
            Tests: invalid format, empty string, and pre-epoch timestamps.
        */
        #[rstest]
        #[case("invalid-timestamp", &["Invalid timestamp", "Failed to parse"])]
        #[case("   ", &["Invalid timestamp", "empty"])]
        #[case("1969-12-31T23:59:59Z", &["Invalid timestamp", "Unix epoch"])]
        #[tokio::test]
        async fn test_write_with_invalid_timestamps(
            context: CliContext,
            #[future] bucket: Bucket,
            #[case] timestamp_str: &str,
            #[case] expected_errors: &[&str],
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;

            let args = write_record_cmd().get_matches_from(vec![
                "write",
                format!("local/{}/test-entry", bucket.name()).as_str(),
                "-s",
                "test payload",
                "-t",
                timestamp_str,
            ]);

            let result = write_handler(&context, &args).await;

            assert!(result.is_err());
            let error_msg = result.unwrap_err().to_string();
            assert!(
                expected_errors.iter().any(|e| error_msg.contains(e)),
                "Expected one of {:?} in error, got: {}",
                expected_errors,
                error_msg
            );

            Ok(())
        }
    }

    mod label_tests {
        use super::*;

        /*
            Test writing records with valid label formats.
            Tests: key=value format, multiple labels, and JSON format.
        */
        #[rstest]
        #[case("env=prod", &[("env", "prod")])]
        #[case("env=prod,version=1.0", &[("env", "prod"), ("version", "1.0")])]
        #[case("key1=value1,key2=value2,key3=value3", &[("key1", "value1"), ("key2", "value2"), ("key3", "value3")])]
        #[case(r#"{"env":"prod"}"#, &[("env", "prod")])]
        #[case(r#"{"env":"prod","version":"1.0"}"#, &[("env", "prod"), ("version", "1.0")])]
        #[tokio::test]
        async fn test_write_string_with_valid_labels(
            context: CliContext,
            #[future] bucket: Bucket,
            #[case] labels_str: &str,
            #[case] expected_labels: &[(&str, &str)],
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;

            let args = write_record_cmd().get_matches_from(vec![
                "write",
                format!("local/{}/test-entry", bucket.name()).as_str(),
                "-s",
                "test payload",
                "--labels",
                labels_str,
            ]);

            write_handler(&context, &args).await?;

            let record = bucket.read_record("test-entry").send().await?;
            let labels = record.labels();

            // Check all expected labels are present
            for (key, value) in expected_labels {
                assert_eq!(
                    labels.get(*key),
                    Some(&value.to_string()),
                    "Expected label {}={}, but got {:?}",
                    key,
                    value,
                    labels.get(*key)
                );
            }

            // Check count matches
            assert_eq!(
                labels.len(),
                expected_labels.len(),
                "Expected {} labels, got {}",
                expected_labels.len(),
                labels.len()
            );

            Ok(())
        }

        /*
            Test that invalid label formats are rejected.
        */
        #[rstest]
        #[case("invalid", "Invalid label format")]
        #[case("=value", "Label key cannot be empty")]
        #[case("key1=value1,invalid", "Invalid label format")]
        #[case(r#"{"invalid json"#, "Failed to parse labels as JSON")]
        #[tokio::test]
        async fn test_write_with_invalid_labels(
            context: CliContext,
            #[future] bucket: Bucket,
            #[case] labels_str: &str,
            #[case] expected_error: &str,
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;

            let args = write_record_cmd().get_matches_from(vec![
                "write",
                format!("local/{}/test-entry", bucket.name()).as_str(),
                "-s",
                "test payload",
                "--labels",
                labels_str,
            ]);

            let result = write_handler(&context, &args).await;

            assert!(result.is_err());
            let error_msg = result.unwrap_err().to_string();
            assert!(
                error_msg.contains(expected_error),
                "Expected error containing '{}', got: {}",
                expected_error,
                error_msg
            );

            Ok(())
        }

        /*
            Test that empty label values are allowed.
        */
        #[rstest]
        #[tokio::test]
        async fn test_write_with_empty_label_value(
            context: CliContext,
            #[future] bucket: Bucket,
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;

            let args = write_record_cmd().get_matches_from(vec![
                "write",
                format!("local/{}/test-entry", bucket.name()).as_str(),
                "-s",
                "test payload",
                "--labels",
                "key=",
            ]);

            write_handler(&context, &args).await?;

            let record = bucket.read_record("test-entry").send().await?;
            let labels = record.labels();

            assert_eq!(labels.get("key"), Some(&"".to_string()));

            Ok(())
        }

        /*
            Test that labels with special characters work correctly.
        */
        #[rstest]
        #[tokio::test]
        async fn test_write_with_special_char_labels(
            context: CliContext,
            #[future] bucket: Bucket,
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;

            let args = write_record_cmd().get_matches_from(vec![
                "write",
                format!("local/{}/test-entry", bucket.name()).as_str(),
                "-s",
                "test payload",
                "--labels",
                "key-1=value_1,key.2=value-2",
            ]);

            write_handler(&context, &args).await?;

            let record = bucket.read_record("test-entry").send().await?;
            let labels = record.labels();

            assert_eq!(labels.get("key-1"), Some(&"value_1".to_string()));
            assert_eq!(labels.get("key.2"), Some(&"value-2".to_string()));

            Ok(())
        }

        /*
            Test writing without labels (labels should be empty).
        */
        #[rstest]
        #[tokio::test]
        async fn test_write_without_labels(
            context: CliContext,
            #[future] bucket: Bucket,
        ) -> anyhow::Result<()> {
            let bucket = bucket.await;

            let args = write_record_cmd().get_matches_from(vec![
                "write",
                format!("local/{}/test-entry", bucket.name()).as_str(),
                "-s",
                "test payload",
            ]);

            write_handler(&context, &args).await?;

            let record = bucket.read_record("test-entry").send().await?;
            let labels = record.labels();

            assert!(labels.is_empty(), "Expected no labels, got: {:?}", labels);

            Ok(())
        }
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
