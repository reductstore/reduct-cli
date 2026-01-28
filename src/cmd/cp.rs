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
    "Source bucket or folder (e.g. SERVER_ALIAS/BUCKET, SERVER_ALIAS/*, SERVER_ALIAS/test-*, or ./folder)";
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

        if let Ok(url) = Url::parse(&value) {
            let scheme = url.scheme();
            if scheme == "http" || scheme == "https" {
                let path = url.path().trim_start_matches('/');
                let bucket = path.trim_end_matches('/');
                if bucket.is_empty() {
                    return Ok(CpPath::Instance(value));
                }
                if bucket.contains('/') {
                    let mut err = Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
                    err.insert(
                        ContextKind::InvalidArg,
                        ContextValue::String(arg.unwrap().to_string()),
                    );
                    err.insert(ContextKind::InvalidValue, ContextValue::String(value));
                    return Err(err);
                }
                let mut base_url = url.clone();
                base_url.set_path("/");
                base_url.set_query(None);
                base_url.set_fragment(None);
                return Ok(CpPath::Bucket {
                    instance: base_url.to_string(),
                    bucket: bucket.to_string(),
                });
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
                .help("Start timestamp (inclusive) in ISO format or Unix timestamp in microseconds.\nIf not specified, the export will start from the first record in each entry.")
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
                .help("Stop timestamp (inclusive) in ISO format or Unix timestamp in microseconds.\nIf not specified, the export will end at the last record in each entry.")
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
                .help("The maximum number of records to export per entry.\nIf not specified, all records will be exported.")
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
            if bucket_wildcard_prefix(&bucket)?.is_some() {
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
            if let Some(prefix) = bucket_wildcard_prefix(&src_bucket)? {
                if prefix.is_empty() {
                    cp_all_buckets(ctx, args, &src_instance, &dst_instance).await?;
                } else {
                    cp_matching_buckets(ctx, args, &src_instance, &dst_instance, prefix).await?;
                }
                return Ok(());
            }
            if src_bucket != "*" {
                return Err(anyhow::anyhow!(
                    "Destination bucket is required (e.g. ALIAS/BUCKET or http://host/BUCKET) unless the source uses '*' to copy all buckets."
                ));
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
            if bucket_wildcard_prefix(&src_bucket)?.is_some()
                || bucket_wildcard_prefix(&dst_bucket)?.is_some()
            {
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

fn bucket_wildcard_prefix(bucket: &str) -> anyhow::Result<Option<&str>> {
    if !bucket.contains('*') {
        return Ok(None);
    }
    if bucket == "*" {
        return Ok(Some(""));
    }
    if bucket.ends_with('*') && bucket.matches('*').count() == 1 {
        return Ok(Some(&bucket[..bucket.len() - 1]));
    }
    Err(anyhow::anyhow!(
        "Bucket wildcard only supports a trailing '*' (e.g. test-*)."
    ))
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

async fn cp_matching_buckets(
    ctx: &CliContext,
    args: &clap::ArgMatches,
    src_instance: &str,
    dst_instance: &str,
    prefix: &str,
) -> anyhow::Result<()> {
    let src_client = build_client(ctx, src_instance).await?;
    let bucket_list = src_client.bucket_list().await?;
    let quiet = args.get_flag("quiet");
    let mut matched = 0usize;

    for bucket in bucket_list
        .buckets
        .into_iter()
        .filter(|bucket| bucket.name.starts_with(prefix))
    {
        matched += 1;
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

    if matched == 0 {
        return Err(anyhow::anyhow!(
            "No buckets matched pattern '{}*' on '{}'.",
            prefix,
            src_instance
        ));
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
    async fn url_without_bucket_parses_as_instance(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "local/bucket", "https://example.com"])
            .unwrap();
        let dst = args
            .get_one::<CpPath>("DESTINATION_BUCKET_OR_FOLDER")
            .unwrap();
        assert!(matches!(dst, CpPath::Instance(value) if value == "https://example.com"));

        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
    }

    #[test]
    fn bucket_wildcard_allows_trailing_star() {
        assert_eq!(bucket_wildcard_prefix("test-*").unwrap(), Some("test-"));
        assert_eq!(bucket_wildcard_prefix("*").unwrap(), Some(""));
    }

    #[test]
    fn bucket_wildcard_rejects_non_trailing_star() {
        assert!(bucket_wildcard_prefix("te*st").is_err());
        assert!(bucket_wildcard_prefix("te**st").is_err());
        assert!(bucket_wildcard_prefix("test-*more").is_err());
    }
}
