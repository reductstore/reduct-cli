// Shared helpers for rendering borderless info tables in CLI commands.
use crate::helpers::timestamp_to_iso;
use tabled::settings::Style;
use tabled::{Table, Tabled};

const DEFAULT_COLUMNS: usize = 2;

#[derive(Tabled)]
struct InfoGridRow {
    #[tabled(rename = "")]
    left: String,
    #[tabled(rename = "")]
    right: String,
}

pub(crate) fn labeled_cell(label: &str, value: impl ToString) -> String {
    format!("{}: {}", label, value.to_string())
}

pub(crate) fn build_info_table(cells: Vec<String>) -> String {
    build_info_table_with_columns(cells, DEFAULT_COLUMNS)
}

pub(crate) fn build_info_table_with_columns(cells: Vec<String>, columns: usize) -> String {
    assert!(columns >= 1, "table must have at least one column");

    let mut rows = Vec::new();
    for chunk in cells.chunks(columns) {
        let left = chunk.get(0).cloned().unwrap_or_default();
        let right = chunk.get(1).cloned().unwrap_or_default();
        rows.push(InfoGridRow { left, right });
    }

    Table::new(rows).with(Style::blank()).to_string()
}

pub(crate) fn record_range_cells(oldest: u64, latest: u64, is_empty: bool) -> Vec<String> {
    let oldest_value = timestamp_to_iso(oldest, is_empty);
    let latest_value = timestamp_to_iso(latest, is_empty);

    vec![
        format!("Oldest Record (UTC): {}", oldest_value),
        String::new(),
        format!("Latest Record (UTC): {}", latest_value),
        String::new(),
    ]
}
