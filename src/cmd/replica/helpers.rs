// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use reduct_rs::ReplicationMode;

pub(crate) fn print_replication_mode(mode: ReplicationMode) -> String {
    match mode {
        ReplicationMode::Enabled => "Enabled".to_string(),
        ReplicationMode::Paused => "Paused".to_string(),
        ReplicationMode::Disabled => "Disabled".to_string(),
    }
}
