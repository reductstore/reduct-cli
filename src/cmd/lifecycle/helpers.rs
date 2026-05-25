// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use reduct_rs::LifecycleMode;

pub(crate) fn format_mode_with_icon(mode: LifecycleMode) -> String {
    match mode {
        LifecycleMode::Enabled => "▶ Enabled".to_string(),
        LifecycleMode::Disabled => "⏹ Disabled".to_string(),
        LifecycleMode::DryRun => "🧪 Dry Run".to_string(),
    }
}
