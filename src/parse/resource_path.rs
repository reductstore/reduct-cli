// Copyright 2023-2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::builder::TypedValueParser;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Arg, Command, Error};
use std::ffi::OsStr;
use url::Url;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Resource {
    Folder(String),
    Alias(String),
    Resource(String, String),
    ResourceWithPath(String, String, String),
}

impl Resource {
    pub(crate) fn pair(self) -> anyhow::Result<(String, String)> {
        match self {
            Resource::Resource(alias_or_url, resource) => Ok((alias_or_url, resource)),
            Resource::ResourceWithPath(alias_or_url, resource, _) => Ok((alias_or_url, resource)),
            Resource::Folder(_) => Err(anyhow::anyhow!("Expected ALIAS/RESOURCE path, got folder")),
            Resource::Alias(_) => Err(anyhow::anyhow!(
                "Expected ALIAS/RESOURCE path, got alias/instance only"
            )),
        }
    }

    pub(crate) fn triple(self) -> anyhow::Result<(String, String, Option<String>)> {
        match self {
            Resource::Resource(alias_or_url, resource) => Ok((alias_or_url, resource, None)),
            Resource::ResourceWithPath(alias_or_url, resource, path) => {
                Ok((alias_or_url, resource, Some(path)))
            }
            Resource::Folder(_) => Err(anyhow::anyhow!("Expected ALIAS/RESOURCE path, got folder")),
            Resource::Alias(_) => Err(anyhow::anyhow!(
                "Expected ALIAS/RESOURCE path, got alias/instance only"
            )),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ResourcePathParser {}

impl TypedValueParser for ResourcePathParser {
    type Value = Resource;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        let value = value.to_string_lossy().to_string();
        let is_folder = [".", "/", ".."].iter().any(|s| value.starts_with(s));
        if is_folder {
            return Ok(Resource::Folder(value));
        }

        if let Ok(mut url) = Url::parse(&value) {
            let scheme = url.scheme();
            if scheme == "http" || scheme == "https" {
                url.set_query(None);
                url.set_fragment(None);

                let path = url.path();
                let has_trailing_slash = path.ends_with('/') && path != "/";
                let segments: Vec<String> = url
                    .path_segments()
                    .map(|segments| {
                        segments
                            .filter(|segment| !segment.is_empty())
                            .map(|s| s.to_string())
                            .collect()
                    })
                    .unwrap_or_default();

                if segments.is_empty() || has_trailing_slash {
                    return Ok(Resource::Alias(url.to_string()));
                }

                let resource = segments
                    .last()
                    .cloned()
                    .ok_or_else(|| Error::new(ErrorKind::ValueValidation).with_cmd(cmd))?;

                let mut base_url = url.clone();
                if segments.len() <= 1 {
                    base_url.set_path("/");
                } else {
                    base_url.set_path(&format!("/{}/", segments[..segments.len() - 1].join("/")));
                }

                return Ok(Resource::Resource(base_url.to_string(), resource));
            }
        }

        let segments: Vec<&str> = value
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect();
        let parsed = match segments.len() {
            0 => {
                let mut err = Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
                err.insert(
                    ContextKind::InvalidArg,
                    ContextValue::String(arg.unwrap().to_string()),
                );
                err.insert(ContextKind::InvalidValue, ContextValue::String(value));
                return Err(err);
            }
            1 => Resource::Alias(segments[0].to_string()),
            2 => Resource::Resource(segments[0].to_string(), segments[1].to_string()),
            _ => Resource::ResourceWithPath(
                segments[0].to_string(),
                segments[1].to_string(),
                segments[2..].join("/"),
            ),
        };

        Ok(parsed)
    }
}

impl ResourcePathParser {
    pub fn new() -> Self {
        Self {}
    }
}
