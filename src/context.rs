// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::io::std::{Output, StdOutput};
use dirs::home_dir;
use std::env::current_dir;
use std::time::Duration;

pub(crate) const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_TIMEOUT_SECS: u64 = 30;
pub(crate) const DEFAULT_PARALLEL: usize = 10;

pub(crate) struct CliContext {
    config_path: String,
    output: Box<dyn Output>,
    ignore_ssl: bool,
    ignore_ssl_overridden: bool,
    timeout: Duration,
    timeout_overridden: bool,
    parallel: usize,
    parallel_overridden: bool,
    ca_cert: Option<String>,
    ca_cert_overridden: bool,
}

impl CliContext {
    pub(crate) fn config_path(&self) -> &str {
        &self.config_path
    }
    pub(crate) fn stdout(&self) -> &dyn Output {
        &*self.output
    }

    pub(crate) fn ignore_ssl(&self) -> bool {
        self.ignore_ssl
    }

    pub(crate) fn ignore_ssl_overridden(&self) -> bool {
        self.ignore_ssl_overridden
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn timeout_overridden(&self) -> bool {
        self.timeout_overridden
    }

    pub(crate) fn parallel(&self) -> usize {
        self.parallel
    }

    pub(crate) fn parallel_overridden(&self) -> bool {
        self.parallel_overridden
    }

    pub(crate) fn ca_cert(&self) -> Option<&String> {
        self.ca_cert.as_ref()
    }

    pub(crate) fn ca_cert_overridden(&self) -> bool {
        self.ca_cert_overridden
    }
}

pub(crate) struct ContextBuilder {
    config: CliContext,
}

impl ContextBuilder {
    pub(crate) fn new() -> Self {
        let mut config = CliContext {
            config_path: String::new(),
            output: Box::new(StdOutput::new()),
            ignore_ssl: false,
            ignore_ssl_overridden: false,
            timeout: DEFAULT_TIMEOUT,
            timeout_overridden: false,
            parallel: DEFAULT_PARALLEL,
            parallel_overridden: false,
            ca_cert: None,
            ca_cert_overridden: false,
        };
        config.config_path = match home_dir() {
            Some(path) => path
                .join(".reduct-cli/config.toml")
                .to_str()
                .unwrap()
                .to_string(),
            None => current_dir()
                .unwrap()
                .join(".reduct-cli/config.toml")
                .to_str()
                .unwrap()
                .to_string(),
        };
        ContextBuilder { config }
    }

    #[allow(dead_code)]
    pub(crate) fn config_path(mut self, config_dir: &str) -> Self {
        self.config.config_path = config_dir.to_string();
        self
    }
    #[allow(dead_code)]
    pub(crate) fn output(mut self, output: Box<dyn Output>) -> Self {
        self.config.output = output;
        self
    }

    pub(crate) fn ignore_ssl(mut self, ignore_ssl: bool) -> Self {
        self.config.ignore_ssl = ignore_ssl;
        self
    }

    pub(crate) fn ignore_ssl_overridden(mut self, overridden: bool) -> Self {
        self.config.ignore_ssl_overridden = overridden;
        self
    }

    pub(crate) fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    pub(crate) fn timeout_overridden(mut self, overridden: bool) -> Self {
        self.config.timeout_overridden = overridden;
        self
    }

    pub(crate) fn parallel(mut self, parallel: usize) -> Self {
        self.config.parallel = parallel;
        self
    }

    pub(crate) fn parallel_overridden(mut self, overridden: bool) -> Self {
        self.config.parallel_overridden = overridden;
        self
    }

    pub(crate) fn ca_cert(mut self, ca_cert: Option<String>) -> Self {
        self.config.ca_cert = ca_cert;
        self
    }

    pub(crate) fn ca_cert_overridden(mut self, overridden: bool) -> Self {
        self.config.ca_cert_overridden = overridden;
        self
    }

    pub(crate) fn build(self) -> CliContext {
        self.config
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::{Alias, ConfigFile};
    use crate::io::reduct::build_client;
    use crate::io::std::Output;
    use reduct_rs::ErrorCode;
    use rstest::fixture;
    use std::cell::RefCell;
    use tokio::time::sleep;

    use tempfile::tempdir;

    pub struct MockOutput {
        history: RefCell<Vec<String>>,
    }

    impl Output for MockOutput {
        fn print(&self, message: &str) {
            self.history.borrow_mut().push(message.to_string());
        }

        fn history(&self) -> Vec<String> {
            self.history.borrow().clone()
        }
    }

    impl MockOutput {
        pub fn new() -> Self {
            MockOutput {
                history: RefCell::new(Vec::new()),
            }
        }
    }

    #[fixture]
    pub(crate) fn output() -> Box<MockOutput> {
        Box::new(MockOutput::new())
    }

    #[fixture]
    pub(crate) fn current_token() -> String {
        std::env::var("RS_API_TOKEN").unwrap_or_default()
    }

    #[fixture]
    pub(crate) fn context(output: Box<dyn Output>, current_token: String) -> CliContext {
        let tmp_dir = tempdir().unwrap();
        let ctx = ContextBuilder::new()
            .config_path(tmp_dir.keep().join("config.toml").to_str().unwrap())
            .output(output)
            .build();

        // add a default alias
        let mut config_file = ConfigFile::load(ctx.config_path()).unwrap();
        let config = config_file.mut_config();
        config.aliases.insert(
            "default".to_string(),
            Alias {
                url: url::Url::parse("https://default.store").unwrap(),
                token: "test_token".to_string(),
                ignore_ssl: false,
                timeout: DEFAULT_TIMEOUT_SECS,
                parallel: DEFAULT_PARALLEL,
                ca_cert: None,
            },
        );
        config.aliases.insert(
            "local".to_string(),
            Alias {
                url: url::Url::parse("http://localhost:8383").unwrap(),
                token: current_token,
                ignore_ssl: false,
                timeout: DEFAULT_TIMEOUT_SECS,
                parallel: DEFAULT_PARALLEL,
                ca_cert: None,
            },
        );
        config_file.save().unwrap();
        ctx
    }

    #[fixture]
    pub(crate) async fn bucket(context: CliContext) -> String {
        let client = build_client(&context, "local").await.unwrap();
        ensure_bucket_absent(&client, "test_bucket").await;

        "test_bucket".to_string()
    }

    #[fixture]
    pub(crate) async fn bucket2(context: CliContext) -> String {
        let client = build_client(&context, "local").await.unwrap();
        ensure_bucket_absent(&client, "test_bucket_2").await;

        "test_bucket_2".to_string()
    }

    async fn ensure_bucket_absent(client: &reduct_rs::ReductClient, name: &str) {
        if let Ok(bucket) = client.get_bucket(name).await {
            let _ = bucket.remove().await;
        }

        for _ in 0..50 {
            match client.get_bucket(name).await {
                Ok(_) => sleep(Duration::from_millis(100)).await,
                Err(err) => {
                    if err.status() == ErrorCode::NotFound {
                        break;
                    }
                    sleep(Duration::from_millis(100)).await
                }
            }
        }
    }

    #[fixture]
    pub(crate) async fn token(context: CliContext) -> String {
        let client = build_client(&context, "local").await.unwrap();
        // remove the token if it already exists
        if let Ok(_) = client.get_token("test_token").await {
            client.delete_token("test_token").await.unwrap_or_default();
        }

        "test_token".to_string()
    }

    #[fixture]
    pub(crate) async fn replica(context: CliContext) -> String {
        let client = build_client(&context, "local").await.unwrap();
        // remove the replica if it already exists
        if let Ok(_) = client.get_replication("test_replica").await {
            client
                .delete_replication("test_replica")
                .await
                .unwrap_or_default();
        }

        "test_replica".to_string()
    }
}
