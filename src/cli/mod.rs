//! Command line interface.

pub mod command;

use std::time::Duration;

use clap::{command, Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Parser)]
#[command(version, about, long_about = None)]
/// Contains the commands
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Get daily data
    Daily {},
    /// Get monthly data
    Monthly {},
    /// Get stations
    Stations {},
}

/// Creates a spinner.
pub fn create_spinner(message: String) -> ProgressBar {
    let bar = ProgressBar::new_spinner().with_message(message);
    bar.enable_steady_tick(Duration::from_millis(100));

    bar
}

/// Creates a progress bar.
pub fn create_progress_bar(size: u64, message: String) -> ProgressBar {
    let bar = ProgressBar::new(size);
    bar.set_style(
        ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {eta}")
            .unwrap()
            .progress_chars("=> "),
    );
    bar.set_message(message);
    bar
}

/// Creates an indeterminate progress bar (for unknown total size).
pub fn create_indeterminate_progress_bar(message: String) -> ProgressBar {
    let bar = ProgressBar::new_spinner();
    bar.set_style(
        ProgressStyle::with_template("{msg} {spinner:.green}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    bar.set_message(message);
    bar.enable_steady_tick(Duration::from_millis(100));
    bar
}

