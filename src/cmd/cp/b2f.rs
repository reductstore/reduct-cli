// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::cp::helpers::{start_loading, CopyVisitor};
use crate::context::CliContext;
use crate::io::reduct::build_client;
use clap::ArgMatches;
use futures_util::StreamExt;
use mime_guess::get_extensions;
use reduct_rs::{ErrorCode, Labels, Record, ReductError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

use tokio::io::AsyncWriteExt;

use crate::parse::parse_query_params;
use tokio::{fs, pin};

struct CopyToFolderVisitor {
    dst_folder: PathBuf,
    ext: Option<String>,
    with_meta: bool,
}

#[derive(Serialize, Deserialize)]
struct Meta {
    timestamp: u64,
    labels: Labels,
    content_type: String,
    content_length: usize,
}

#[async_trait::async_trait]
impl CopyVisitor for CopyToFolderVisitor {
    async fn visit(
        &self,
        entry_name: &str,
        records: Vec<Record>,
    ) -> Result<BTreeMap<u64, ReductError>, ReductError> {
        let mut result: BTreeMap<u64, ReductError> = BTreeMap::new();

        if records.len() == 1 {
            let record = records.into_iter().next().unwrap();
            let timestamp = record.timestamp_us();
            let res = self.visit_one_record(&entry_name, record).await;
            if let Err(err) = res {
                result.insert(timestamp, err);
            }
            Ok(result)
        } else {
            for record in records {
                let timestamp = record.timestamp_us();
                let res = self.visit_one_record(entry_name, record).await;
                if let Err(err) = res {
                    result.insert(timestamp, err);
                }
            }
            Ok(result)
        }
    }
}

impl CopyToFolderVisitor {
    fn guess_extension(record: &Record) -> String {
        if let Some((top, sub)) = record.content_type().split_once('/') {
            if let Some(well_known) = Self::well_known_ext(top, sub) {
                well_known.to_string()
            } else if let Some(ext) = get_extensions(top, sub) {
                ext.first()
                    .map_or_else(|| "bin".to_string(), |e| e.to_string())
            } else {
                "bin".to_string()
            }
        } else {
            "bin".to_string()
        }
    }

    fn well_known_ext(top: &str, sub: &str) -> Option<&'static str> {
        match (top, sub) {
            ("application", "octet-stream") => Some("bin"),
            ("text", "html") => Some("html"),
            ("text", "markdown") => Some("md"),
            ("text", "plain") => Some("txt"),
            ("text", "csv") => Some("csv"),
            ("application", "json") => Some("json"),
            ("application", "xml") => Some("xml"),
            ("application", "pdf") => Some("pdf"),
            ("application", "csv") => Some("csv"),
            ("application", "mcap") => Some("mcap"),
            ("image", "jpeg") => Some("jpg"),
            ("image", "png") => Some("png"),
            _ => None,
        }
    }

    async fn visit_one_record(&self, entry_name: &str, record: Record) -> Result<(), ReductError> {
        fs::create_dir_all(&self.dst_folder.join(entry_name)).await?;

        let ext = if let Some(ext) = &self.ext {
            ext.clone()
        } else {
            Self::guess_extension(&record)
        };

        let file_path =
            self.dst_folder
                .join(entry_name)
                .join(format!("{}.{}", record.timestamp_us(), ext));
        let mut file = fs::File::create(file_path).await?;

        let meta = Meta {
            timestamp: record.timestamp_us(),
            labels: record.labels().clone(),
            content_type: record.content_type().to_string(),
            content_length: record.content_length(),
        };

        let stream = record.stream_bytes();
        pin!(stream);
        let mut count = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            count += chunk.len();
        }

        if count != meta.content_length {
            return Err(ReductError::new(
                ErrorCode::Unknown,
                "Content length mismatch",
            ));
        }

        if self.with_meta {
            let meta_path = self
                .dst_folder
                .join(entry_name)
                .join(format!("{}-meta.json", meta.timestamp));
            let mut meta_file = fs::File::create(meta_path).await?;
            let meta = serde_json::to_string_pretty(&meta)
                .map_err(|err| ReductError::new(ErrorCode::Unknown, &err.to_string()))?;
            meta_file.write_all(meta.as_bytes()).await?;
        }

        Ok(())
    }
}

pub(crate) async fn cp_bucket_to_folder(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (src_instance, src_bucket) = args
        .get_one::<(String, String)>("SOURCE_BUCKET_OR_FOLDER")
        .map(|(src_instance, src_bucket)| (src_instance.clone(), src_bucket.clone()))
        .unwrap();
    let (first_folder, rest_path) = args
        .get_one::<(String, String)>("DESTINATION_BUCKET_OR_FOLDER")
        .map(|(dst_instance, dst_bucket)| (dst_instance.clone(), dst_bucket.clone()))
        .unwrap();

    let query_params = parse_query_params(ctx, &args)?;
    let src_bucket = build_client(ctx, &src_instance)
        .await?
        .get_bucket(&src_bucket)
        .await?;

    let dst_folder = PathBuf::from(first_folder).join(rest_path);
    let visitor = CopyToFolderVisitor {
        dst_folder,
        ext: args.get_one::<String>("ext").map(|ext| ext.to_string()),
        with_meta: args.get_one::<bool>("with-meta").unwrap().clone(),
    };

    start_loading(src_bucket, query_params, visitor).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::cp::cp_cmd;
    use crate::context::tests::{bucket, context};
    use bytes::Bytes;
    use reduct_rs::RecordBuilder;
    use rstest::*;
    use tempfile::tempdir;

    mod visitor {
        use super::*;
        use tempfile::tempdir;

        #[rstest]
        #[tokio::test]
        async fn test_copy_to_folder_visitor(
            visitor: CopyToFolderVisitor,
            entry_name: String,
            records: Vec<Record>,
        ) {
            let result = visitor.visit(&entry_name, records).await;
            assert!(result.is_ok());

            let file_path = PathBuf::from(visitor.dst_folder)
                .join(&entry_name)
                .join("1234567890.html");
            assert!(file_path.exists());
            assert_eq!(
                fs::read_to_string(file_path).await.unwrap(),
                "Hello, World!"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_copy_to_folder_visitor_ext(
            mut visitor: CopyToFolderVisitor,
            entry_name: String,
            records: Vec<Record>,
        ) {
            visitor.ext = Some("md".to_string());
            let result = visitor.visit(&entry_name, records).await;
            assert!(result.is_ok());

            let file_path = PathBuf::from(visitor.dst_folder)
                .join(&entry_name)
                .join("1234567890.md");
            assert!(file_path.exists());
        }

        #[rstest]
        #[tokio::test]
        async fn test_copy_to_folder_visitor_with_meta(
            mut visitor: CopyToFolderVisitor,
            entry_name: String,
            records: Vec<Record>,
        ) {
            visitor.with_meta = true;
            let result = visitor.visit(&entry_name, records).await;
            assert!(result.is_ok());

            let file_path = PathBuf::from(visitor.dst_folder.clone())
                .join(&entry_name)
                .join("1234567890.html");
            assert!(file_path.exists());
            assert_eq!(
                fs::read_to_string(file_path).await.unwrap(),
                "Hello, World!"
            );

            let meta_path = PathBuf::from(visitor.dst_folder)
                .join(&entry_name)
                .join("1234567890-meta.json");
            assert!(meta_path.exists());

            let meta = fs::read_to_string(meta_path).await.unwrap();
            let meta: Meta = serde_json::from_str(&meta).unwrap();
            assert_eq!(meta.timestamp, 1234567890);
            assert_eq!(meta.content_type, "text/html");
            assert_eq!(meta.content_length, 13);
            assert_eq!(meta.labels["planet"], "Earth");
            assert_eq!(meta.labels["greeting"], "Hello");
        }

        #[fixture]
        fn visitor() -> CopyToFolderVisitor {
            CopyToFolderVisitor {
                dst_folder: tempdir().unwrap().keep(),
                ext: None,
                with_meta: false,
            }
        }

        #[fixture]
        fn entry_name() -> String {
            "test".to_string()
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_cp_bucket_to_folder(
        context: CliContext,
        #[future] bucket: String,
        records: Vec<Record>,
    ) {
        let client = build_client(&context, "local").await.unwrap();
        let src_bucket = client.create_bucket(&bucket.await).send().await.unwrap();

        let timestamp = records[0].timestamp_us();
        let content_type = records[0].content_type().to_string();
        let labels = records[0].labels().clone();
        let data = Bytes::from_static(b"Hello, World!");

        src_bucket
            .write_record("test")
            .timestamp_us(timestamp)
            .content_type(&*content_type)
            .labels(labels)
            .data(data)
            .send()
            .await
            .unwrap();

        let path = tempdir().unwrap().keep();
        let args = cp_cmd()
            .try_get_matches_from(vec![
                "cp",
                format!("local/{}", src_bucket.name()).as_str(),
                format!("{}", path.to_string_lossy()).as_str(),
            ])
            .unwrap();

        cp_bucket_to_folder(&context, &args).await.unwrap();

        let file_path = path.join("test").join("1234567890.html");
        assert!(file_path.exists());
        assert_eq!(
            fs::read_to_string(file_path).await.unwrap(),
            "Hello, World!"
        );
    }

    #[rstest]
    #[case("application/octet-stream", "bin")]
    #[case("text/html", "html")]
    #[case("text/markdown", "md")]
    #[case("text/plain", "txt")]
    #[case("text/csv", "csv")]
    #[case("application/json", "json")]
    #[case("application/xml", "xml")]
    #[case("application/pdf", "pdf")]
    #[case("application/csv", "csv")]
    #[case("application/mcap", "mcap")]
    #[case("image/jpeg", "jpg")]
    #[case("image/png", "png")]
    #[case("image/gif", "gif")]
    fn test_guess_extension(#[case] content_type: &str, #[case] ext: &str) {
        let record = RecordBuilder::new()
            .content_type(content_type.to_string())
            .build();
        assert_eq!(CopyToFolderVisitor::guess_extension(&record), ext);
    }

    #[fixture]
    fn records() -> Vec<Record> {
        vec![RecordBuilder::new()
            .timestamp_us(1234567890)
            .data(Bytes::from_static(b"Hello, World!"))
            .content_type("text/html".to_string())
            .add_label("planet".to_string(), "Earth".to_string())
            .add_label("greeting".to_string(), "Hello".to_string())
            .build()]
    }
}
