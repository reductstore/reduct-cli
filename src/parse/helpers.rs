// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use reduct_rs::{Bucket, EntryInfo};

pub(crate) async fn fetch_and_filter_entries(
    src_bucket: &Bucket,
    entry_filter: &Vec<String>,
) -> anyhow::Result<Vec<EntryInfo>> {
    let entries = src_bucket
        .entries()
        .await?
        .iter()
        .filter(|entry| -> bool {
            if entry_filter.is_empty() || entry_filter.contains(&entry.name) {
                true
            } else {
                // check wildcard
                entry_filter.iter().any(|filter| {
                    filter.ends_with('*') && entry.name.starts_with(&filter[..filter.len() - 1])
                })
            }
        })
        .map(|entry| entry.clone())
        .collect::<Vec<EntryInfo>>();
    Ok(entries)
}
