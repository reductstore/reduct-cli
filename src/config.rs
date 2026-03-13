// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::context::CliContext;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

use std::path::PathBuf;
use url::Url;

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub(crate) struct Alias {
    pub url: Url,
    pub token: String,
    #[serde(default)]
    pub ignore_ssl: bool,
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    #[serde(default = "default_parallel")]
    pub parallel: usize,
    #[serde(default)]
    pub ca_cert: Option<String>,
}

fn default_timeout() -> u64 {
    crate::context::DEFAULT_TIMEOUT_SECS
}

fn default_parallel() -> usize {
    crate::context::DEFAULT_PARALLEL
}

#[derive(Deserialize, Serialize, PartialEq, Debug)]
pub(crate) struct Config {
    pub aliases: BTreeMap<String, Alias>,
}

pub(crate) struct ConfigFile {
    path: PathBuf,
    config: Config,
}

impl ConfigFile {
    pub fn load(path: &str) -> anyhow::Result<ConfigFile> {
        let config: anyhow::Result<Config> = match std::fs::read_to_string(path) {
            Ok(config) => Ok(toml::from_str(&config)
                .with_context(|| format!("Failed to parse config file {:?}", path))?),
            Err(_) => Ok(Config {
                aliases: BTreeMap::new(),
            }),
        };

        Ok(ConfigFile {
            path: PathBuf::from(path),
            config: config?,
        })
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let config = toml::to_string(&self.config)?;

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create config directory {}", parent.display())
            })?;
        }

        std::fs::write(&self.path, config)
            .with_context(|| format!("Failed to write config file {:?}", &self.path,))?;
        Ok(())
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn mut_config(&mut self) -> &mut Config {
        &mut self.config
    }
}

pub(crate) fn find_alias(ctx: &CliContext, alias: &str) -> anyhow::Result<Alias> {
    let config_file = ConfigFile::load(ctx.config_path())?;
    let config = config_file.config();
    match config.aliases.get(alias) {
        Some(alias) => Ok((*alias).clone()),
        None => Err(anyhow::Error::msg(format!(
            "Alias '{}' does not exist",
            alias
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConnectionOptions {
    pub ignore_ssl: bool,
    pub timeout: Duration,
    pub parallel: usize,
    pub ca_cert: Option<String>,
}

pub(crate) fn resolve_connection_options(
    ctx: &CliContext,
    alias_or_url: &str,
) -> ConnectionOptions {
    let alias_options = find_alias(ctx, alias_or_url).ok();
    ConnectionOptions {
        ignore_ssl: if ctx.ignore_ssl_overridden() {
            ctx.ignore_ssl()
        } else {
            alias_options
                .as_ref()
                .map(|alias| alias.ignore_ssl)
                .unwrap_or(ctx.ignore_ssl())
        },
        timeout: if ctx.timeout_overridden() {
            ctx.timeout()
        } else {
            alias_options
                .as_ref()
                .map(|alias| Duration::from_secs(alias.timeout))
                .unwrap_or(ctx.timeout())
        },
        parallel: if ctx.parallel_overridden() {
            ctx.parallel()
        } else {
            alias_options
                .as_ref()
                .map(|alias| alias.parallel)
                .unwrap_or(ctx.parallel())
        },
        ca_cert: if ctx.ca_cert_overridden() {
            ctx.ca_cert().cloned()
        } else {
            alias_options
                .as_ref()
                .and_then(|alias| alias.ca_cert.clone())
                .or_else(|| ctx.ca_cert().cloned())
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use std::time::Duration;

    use crate::context::tests::{context, output};
    use crate::context::{CliContext, ContextBuilder};

    #[rstest]
    fn test_load(context: CliContext) {
        let mut file = File::create(context.config_path()).unwrap();
        file.write_all(
            r#"
            [aliases]
            test = { url = "https://test.com", token = "test" }
            "#
            .as_bytes(),
        )
        .unwrap();

        let config_file = ConfigFile::load(context.config_path()).unwrap();
        let config = config_file.config();
        assert_eq!(config.aliases.len(), 1);
        assert_eq!(
            config.aliases.get("test").unwrap().url.as_str(),
            "https://test.com/"
        );
        assert_eq!(config.aliases.get("test").unwrap().token, "test");
        assert_eq!(config.aliases.get("test").unwrap().timeout, 30);
        assert_eq!(config.aliases.get("test").unwrap().parallel, 10);
        assert_eq!(config.aliases.get("test").unwrap().ca_cert, None);
    }

    #[rstest]
    fn test_save(context: CliContext) {
        let mut config_file = ConfigFile::load(context.config_path()).unwrap();
        let config = config_file.mut_config();
        config.aliases = vec![(
            "test".to_string(),
            Alias {
                url: Url::parse("https://test.com").unwrap(),
                token: "test".to_string(),
                ignore_ssl: false,
                timeout: 30,
                parallel: 10,
                ca_cert: None,
            },
        )]
        .into_iter()
        .collect();

        config_file.save().unwrap();
        let cfg: Config =
            toml::from_str(&fs::read_to_string(context.config_path()).unwrap()).unwrap();
        assert_eq!(
            cfg.aliases.get("test").unwrap().url.as_str(),
            "https://test.com/"
        );
    }

    #[rstest]
    fn test_empty_config(context: CliContext) {
        let config_file = ConfigFile::load(&format!("{}.empty", context.config_path())).unwrap();
        assert_eq!(config_file.config().aliases.len(), 0);
    }

    #[rstest]
    fn test_resolve_connection_options_from_alias(context: CliContext) {
        let mut config_file = ConfigFile::load(context.config_path()).unwrap();
        let alias = config_file.mut_config().aliases.get_mut("default").unwrap();
        alias.ignore_ssl = true;
        alias.timeout = 7;
        alias.parallel = 2;
        alias.ca_cert = Some("/tmp/test.crt".to_string());
        config_file.save().unwrap();

        let options = resolve_connection_options(&context, "default");
        assert_eq!(options.ignore_ssl, true);
        assert_eq!(options.timeout, Duration::from_secs(7));
        assert_eq!(options.parallel, 2);
        assert_eq!(options.ca_cert, Some("/tmp/test.crt".to_string()));
    }

    #[rstest]
    fn test_resolve_connection_options_cli_override(
        context: CliContext,
        output: Box<dyn crate::io::std::Output>,
    ) {
        let ctx = ContextBuilder::new()
            .config_path(context.config_path())
            .output(output)
            .ignore_ssl(false)
            .ignore_ssl_overridden(false)
            .timeout(Duration::from_secs(5))
            .timeout_overridden(true)
            .parallel(4)
            .parallel_overridden(true)
            .ca_cert(Some("/tmp/override.crt".to_string()))
            .ca_cert_overridden(true)
            .build();

        let options = resolve_connection_options(&ctx, "default");
        assert_eq!(options.timeout, Duration::from_secs(5));
        assert_eq!(options.parallel, 4);
        assert_eq!(options.ca_cert, Some("/tmp/override.crt".to_string()));
    }
}
