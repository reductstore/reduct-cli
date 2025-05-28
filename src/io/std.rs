// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
pub(crate) trait Output {
    fn print(&self, message: &str);

    #[cfg(test)]
    fn history(&self) -> Vec<String>;
}

pub(crate) struct StdOutput;

impl StdOutput {
    pub(crate) fn new() -> Self {
        StdOutput {}
    }
}

impl Output for StdOutput {
    fn print(&self, message: &str) {
        println!("{}", message);
    }

    #[cfg(test)]
    fn history(&self) -> Vec<String> {
        Vec::new()
    }
}

macro_rules! output {
    ($ctx:expr, $($arg:tt)*) => {
        $ctx.stdout().print(&format!($($arg)*));
    };
}

pub(crate) use output;
