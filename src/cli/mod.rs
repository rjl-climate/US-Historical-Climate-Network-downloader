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
}

/// Creates a spinner.
pub fn create_spinner(message: String) -> ProgressBar {
    let new_spinner = ProgressBar::new_spinner();
    let bar = new_spinner.with_message(message.clone());
    bar.enable_steady_tick(Duration::from_millis(100));
    bar
}

/// Creates a progress bar.
pub fn create_progress_bar(size: u64, message: String) -> ProgressBar {
    let progress_bar = ProgressBar::new(size).with_message(message).with_style(
        ProgressStyle::with_template("[{eta_precise}] {bar:40.cyan/blue} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );
    progress_bar
}
