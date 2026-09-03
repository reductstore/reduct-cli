// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::ConfigFile;
use crate::context::CliContext;
use crate::io::std::output;
use clap::Command;

pub(super) fn list_aliases(ctx: &CliContext) -> anyhow::Result<()> {
    let config_file = ConfigFile::load(ctx.config_path())?;
    let config = config_file.config();
    if ctx.json() {
        let aliases = &config.aliases;
        output!(ctx, "{}", serde_json::to_string(&aliases)?);
    } else {
        for (name, alias) in config.aliases.iter() {
            output!(ctx, "{}: {}", name, alias.url);
        }
    }

    Ok(())
}

pub(super) fn ls_aliases_cmd() -> Command {
    Command::new("ls").about("List all aliases")
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::context::{
        tests::{context, MockOutput},
        ContextBuilder,
    };

    use rstest::rstest;

    #[rstest]
    fn test_list_aliases(context: CliContext) {
        list_aliases(&context).unwrap();
        assert_eq!(
            context.stdout().history(),
            vec![
                "default: https://default.store/",
                "local: http://localhost:8383/"
            ]
        );
    }

    #[rstest]
    fn test_list_aliases_json(context: CliContext) {
        let ctx = ContextBuilder::new()
            .config_path(context.config_path())
            .json(Some(true))
            .output(Box::new(MockOutput::new()))
            .build();

        list_aliases(&ctx).unwrap();

        let aliases_json: serde_json::Value =
            serde_json::from_str(&ctx.stdout().history()[0]).unwrap();

        assert_eq!(
            aliases_json["default"]["url"],
            serde_json::json!("https://default.store/")
        );
        assert_eq!(
            aliases_json["local"]["url"],
            serde_json::json!("http://localhost:8383/")
        );
    }
}
