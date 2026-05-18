// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::Resource;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};

pub(super) fn rm_lifecycle_cmd() -> Command {
    Command::new("rm")
        .about("Remove a lifecycle policy")
        .arg(
            Arg::new("LIFECYCLE_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(crate::parse::ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("yes")
                .long("yes")
                .short('y')
                .help("Do not ask for confirmation")
                .action(SetTrue)
                .required(false),
        )
}

pub(super) async fn rm_lifecycle_handler(
    ctx: &CliContext,
    args: &ArgMatches,
) -> anyhow::Result<()> {
    let (alias_or_url, lifecycle_name) = args
        .get_one::<Resource>("LIFECYCLE_PATH")
        .unwrap()
        .clone()
        .pair()?;

    let confirm = if !args.get_flag("yes") {
        dialoguer::Confirm::new()
            .default(false)
            .with_prompt(format!(
                "Are you sure you want to delete lifecycle '{}'?",
                lifecycle_name
            ))
            .interact()?
    } else {
        true
    };

    if confirm {
        let client = build_client(ctx, &alias_or_url).await?;
        client.delete_lifecycle(&lifecycle_name).await?;
        output!(ctx, "Lifecycle '{}' deleted", lifecycle_name);
    } else {
        output!(ctx, "Lifecycle '{}' not deleted", lifecycle_name);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::cmd::lifecycle::rm::{rm_lifecycle_cmd, rm_lifecycle_handler};
    use crate::cmd::lifecycle::tests::{prepare_lifecycle, unique_name};
    use crate::context::tests::context;
    use crate::context::CliContext;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_rm_lifecycle(context: CliContext) {
        let lifecycle = unique_name("test-lifecycle");
        let bucket = unique_name("test-bucket");

        let client = prepare_lifecycle(&context, &lifecycle, &bucket)
            .await
            .unwrap();

        let args = rm_lifecycle_cmd()
            .try_get_matches_from(vec!["rm", format!("local/{}", lifecycle).as_str(), "--yes"])
            .unwrap();
        rm_lifecycle_handler(&context, &args).await.unwrap();

        assert_eq!(
            client
                .get_lifecycle(&lifecycle)
                .await
                .err()
                .unwrap()
                .to_string(),
            format!("[NotFound] Lifecycle '{}' does not exist", lifecycle)
        );
    }
}
