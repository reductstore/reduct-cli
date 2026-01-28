// Copyright 2024-2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod b2b;
mod b2f;
mod helpers;

use crate::cmd::cp::b2b::{cp_bucket_to_bucket, cp_bucket_to_bucket_with};
use crate::cmd::cp::b2f::cp_bucket_to_folder;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::widely_used_args::{
    make_each_n, make_each_s, make_entries_arg, make_ext_arg, make_strict_arg, make_when_arg,
};
use clap::builder::TypedValueParser;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::ArgAction::SetTrue;
use clap::{value_parser, Arg, Command, Error};
use std::ffi::OsStr;
use url::Url;

const CP_SOURCE_HELP: &str =
    "Source bucket or folder (e.g. SERVER_ALIAS/BUCKET, SERVER_ALIAS/*, or ./folder)";
const CP_DEST_HELP: &str =
    "Destination bucket, instance, or folder (e.g. SERVER_ALIAS/BUCKET, SERVER_ALIAS, or ./folder)";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CpPath {
    Folder(String),
    Bucket { instance: String, bucket: String },
    Instance(String),
}

#[derive(Clone)]
struct CpPathParser;

impl TypedValueParser for CpPathParser {
    type Value = CpPath;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        let value = value.to_string_lossy().to_string();
        let is_folder = [".", "/", ".."].iter().any(|s| value.starts_with(s));
        if is_folder {
            return Ok(CpPath::Folder(value));
        }

        // Check if it's a URL (http:// or https://)
        let is_url = value.starts_with("http://") || value.starts_with("https://");
        
        if is_url {
            // Parse as URL to extract path properly
            match Url::parse(&value) {
                Ok(parsed_url) => {
                    let path = parsed_url.path();
                    // Remove leading slash and check if there's a bucket name
                    let path_without_leading_slash = path.trim_start_matches('/');
                    
                    // Helper to reconstruct base URL without path
                    let reconstruct_base_url = || {
                        let base = format!("{}://{}", parsed_url.scheme(), parsed_url.host_str().unwrap_or(""));
                        if let Some(port) = parsed_url.port() {
                            format!("{}:{}", base, port)
                        } else {
                            base
                        }
                    };
                    
                    if path_without_leading_slash.is_empty() {
                        // URL without a path - return as Instance
                        return Ok(CpPath::Instance(reconstruct_base_url()));
                    } else {
                        // URL with a path - extract bucket from first path segment
                        let bucket = path_without_leading_slash.split('/').next().unwrap().to_string();
                        return Ok(CpPath::Bucket {
                            instance: reconstruct_base_url(),
                            bucket,
                        });
                    }
                }
                Err(_) => {
                    // If URL parsing fails for something that looks like a URL, return a clear error
                    let mut err = Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
                    if let Some(arg) = arg {
                        err.insert(
                            ContextKind::InvalidArg,
                            ContextValue::String(arg.to_string()),
                        );
                    }
                    err.insert(
                        ContextKind::InvalidValue, 
                        ContextValue::String(format!("Invalid URL format: {}", value))
                    );
                    return Err(err);
                }
            }
        }

        if let Some((alias_or_url, resource_name)) = value.rsplit_once('/') {
            if resource_name.is_empty() {
                return Ok(CpPath::Instance(alias_or_url.to_string()));
            }
            Ok(CpPath::Bucket {
                instance: alias_or_url.to_string(),
                bucket: resource_name.to_string(),
            })
        } else if value.is_empty() {
            let mut err = Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
            err.insert(
                ContextKind::InvalidArg,
                ContextValue::String(arg.unwrap().to_string()),
            );
            err.insert(ContextKind::InvalidValue, ContextValue::String(value));
            Err(err)
        } else {
            Ok(CpPath::Instance(value))
        }
    }
}

pub(crate) fn cp_cmd() -> Command {
    Command::new("cp")
        .about("Copy data between instances or between an instance and the local filesystem")
        .arg_required_else_help(true)
        .arg(
            Arg::new("SOURCE_BUCKET_OR_FOLDER")
                .help(CP_SOURCE_HELP)
                .value_parser(CpPathParser)
                .required(true),
        )
        .arg(
            Arg::new("DESTINATION_BUCKET_OR_FOLDER")
                .help(CP_DEST_HELP)
                .value_parser(CpPathParser)
                .required(true),
        )
        .arg(
            Arg::new("start")
                .long("start")
                .short('b')
                .help("Export records  with timestamps older than this time point in ISO format or Unix timestamp in microseconds.\nIf not specified, the export will start from the first record in an entry.")
                .required(false)
        )
        .arg(
            Arg::new("from-last")
                .long("from-last")
                .help("Copy records starting after the latest record in each destination entry.")
                .required(false)
                .action(SetTrue)
        )
        .arg(
            Arg::new("stop")
                .long("stop")
                .short('e')
                .help("Export records with timestamps newer than this time point in ISO format or Unix timestamp in microseconds.\nIf not specified, the export will end at the last record in an entry.")
                .required(false)
        )
        .arg(make_entries_arg())
        .arg(make_when_arg())
        .arg(make_strict_arg())
        .arg(make_each_n())
        .arg(make_each_s())
        .arg(
            Arg::new("limit")
                .long("limit")
                .short('l')
                .help("The maximum number of records to export.\nIf not specified, all records will be exported.")
                .value_name("NUMBER")
                .value_parser(value_parser!(u64))
                .required(false)
        )
        .arg(
            Arg::new("ext")
                .long("ext")
                .short('x')
                .help("The file extension to use for the exported file.\nIf not specified, the default be guessed from the content type of the records.")
                .value_name("TEXT")
                .required(false)
        )
        .arg(
            Arg::new("with-meta")
                .long("with-meta")
                .short('m')
                .help("Export the metadata of the records along with the records in JSON format.\nIf not specified, only the records will be exported.")
                .required(false)
                .action(SetTrue)
        )
        .arg(
            Arg::new("quiet")
                .long("quiet")
                .short('q')
                .help("Only show errors and the final completion message.")
                .required(false)
                .action(SetTrue)
        )
        .arg(
            make_ext_arg()
        )
}

pub(crate) async fn cp_handler(ctx: &CliContext, args: &clap::ArgMatches) -> anyhow::Result<()> {
    let src = args
        .get_one::<CpPath>("SOURCE_BUCKET_OR_FOLDER")
        .unwrap()
        .clone();
    let dst = args
        .get_one::<CpPath>("DESTINATION_BUCKET_OR_FOLDER")
        .unwrap()
        .clone();

    match (src, dst) {
        (CpPath::Folder(_), CpPath::Folder(_)) => {
            Err(anyhow::anyhow!("Folder to folder copy is not supported."))
        }
        (CpPath::Folder(_), CpPath::Bucket { .. } | CpPath::Instance(_)) => {
            Err(anyhow::anyhow!("Folder to bucket copy is not supported."))
        }
        (CpPath::Bucket { bucket, .. }, CpPath::Folder(_)) => {
            if bucket == "*" {
                return Err(anyhow::anyhow!(
                    "Wildcard bucket copy requires the destination to be an instance only."
                ));
            }
            cp_bucket_to_folder(ctx, args).await?;
            Ok(())
        }
        (
            CpPath::Bucket {
                instance: src_instance,
                bucket: src_bucket,
            },
            CpPath::Instance(dst_instance),
        ) => {
            if src_bucket != "*" {
                // Check if the destination is a URL to provide a more helpful error message
                let is_dst_url = dst_instance.starts_with("http://") || dst_instance.starts_with("https://");
                if is_dst_url {
                    return Err(anyhow::anyhow!(
                        "URL destination must include a path to the bucket (e.g., {}/BUCKET_NAME). Use '*' for the source bucket to copy all buckets to the destination instance.",
                        dst_instance.trim_end_matches('/')
                    ));
                } else {
                    return Err(anyhow::anyhow!(
                        "Destination bucket is required unless the source uses '*' to copy all buckets."
                    ));
                }
            }

            cp_all_buckets(ctx, args, &src_instance, &dst_instance).await?;
            Ok(())
        }
        (
            CpPath::Bucket {
                bucket: src_bucket, ..
            },
            CpPath::Bucket {
                bucket: dst_bucket, ..
            },
        ) => {
            if src_bucket == "*" || dst_bucket == "*" {
                return Err(anyhow::anyhow!(
                    "Wildcard bucket copy requires the destination to be an instance only."
                ));
            }
            cp_bucket_to_bucket(ctx, args).await?;
            Ok(())
        }
        (CpPath::Instance(_), _) => Err(anyhow::anyhow!(
            "Source must include a bucket name or use '*' for all buckets."
        )),
    }
}

async fn cp_all_buckets(
    ctx: &CliContext,
    args: &clap::ArgMatches,
    src_instance: &str,
    dst_instance: &str,
) -> anyhow::Result<()> {
    let src_client = build_client(ctx, src_instance).await?;
    let bucket_list = src_client.bucket_list().await?;
    let quiet = args.get_flag("quiet");

    for bucket in bucket_list.buckets {
        if !quiet {
            output!(
                ctx,
                "Copying bucket '{}' from '{}' to '{}'",
                bucket.name,
                src_instance,
                dst_instance
            );
        }
        cp_bucket_to_bucket_with(
            ctx,
            args,
            src_instance,
            &bucket.name,
            dst_instance,
            &bucket.name,
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::context;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn folder_to_folder_unsupported(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "./", "./"])
            .unwrap();
        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn folder_to_bucket_unsupported(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "./", "local/bucket"])
            .unwrap();
        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn bucket_to_instance_without_wildcard_rejected(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "local/bucket", "local"])
            .unwrap();
        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn wildcard_to_bucket_rejected(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "local/*", "local/bucket"])
            .unwrap();
        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn instance_source_rejected(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "local", "local/bucket"])
            .unwrap();
        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn url_without_bucket_provides_clear_error(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "local/bucket", "https://test.example.com"])
            .unwrap();
        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("URL destination must include a path to the bucket"));
        assert!(error_msg.contains("https://test.example.com/BUCKET_NAME"));
    }

    #[rstest]
    #[tokio::test]
    async fn url_with_trailing_slash_provides_clear_error(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "local/bucket", "https://test.example.com/"])
            .unwrap();
        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("URL destination must include a path to the bucket"));
    }

    #[test]
    fn test_parse_url_with_bucket() {
        let parser = CpPathParser;
        let cmd = cp_cmd();
        let result = parser.parse_ref(&cmd, None, OsStr::new("https://test.example.com/my-bucket"));
        assert!(result.is_ok());
        match result.unwrap() {
            CpPath::Bucket { instance, bucket } => {
                assert_eq!(instance, "https://test.example.com");
                assert_eq!(bucket, "my-bucket");
            }
            _ => panic!("Expected Bucket variant"),
        }
    }

    #[test]
    fn test_parse_url_without_bucket() {
        let parser = CpPathParser;
        let cmd = cp_cmd();
        let result = parser.parse_ref(&cmd, None, OsStr::new("https://test.example.com"));
        assert!(result.is_ok());
        match result.unwrap() {
            CpPath::Instance(instance) => {
                assert_eq!(instance, "https://test.example.com");
            }
            _ => panic!("Expected Instance variant"),
        }
    }

    #[test]
    fn test_parse_url_with_port_and_bucket() {
        let parser = CpPathParser;
        let cmd = cp_cmd();
        let result = parser.parse_ref(&cmd, None, OsStr::new("http://localhost:8383/bucket"));
        assert!(result.is_ok());
        match result.unwrap() {
            CpPath::Bucket { instance, bucket } => {
                assert_eq!(instance, "http://localhost:8383");
                assert_eq!(bucket, "bucket");
            }
            _ => panic!("Expected Bucket variant"),
        }
    }

    #[test]
    fn test_parse_url_with_multiple_path_segments() {
        let parser = CpPathParser;
        let cmd = cp_cmd();
        // Only the first path segment should be used as the bucket name
        let result = parser.parse_ref(&cmd, None, OsStr::new("https://example.com/bucket/extra/path"));
        assert!(result.is_ok());
        match result.unwrap() {
            CpPath::Bucket { instance, bucket } => {
                assert_eq!(instance, "https://example.com");
                assert_eq!(bucket, "bucket"); // Only first segment
            }
            _ => panic!("Expected Bucket variant"),
        }
    }

    #[test]
    fn test_parse_invalid_url() {
        let parser = CpPathParser;
        let cmd = cp_cmd();
        // Invalid URL with spaces should return an error
        let result = parser.parse_ref(&cmd, None, OsStr::new("https://invalid url with spaces"));
        assert!(result.is_err());
    }
}
