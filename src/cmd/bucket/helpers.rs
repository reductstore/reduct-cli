// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use reduct_rs::ResourceStatus;

pub(super) fn print_bucket_status(status: &ResourceStatus) -> String {
    match status {
        ResourceStatus::Ready => "Ready".to_string(),
        ResourceStatus::Deleting => "Deleting".to_string(),
    }
}
