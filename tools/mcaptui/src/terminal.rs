use std::{
    io::{self, Stdout},
    ops::{Deref, DerefMut},
};

use anyhow::Result;
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};

type TuiTerminal = Terminal<CrosstermBackend<Stdout>>;

pub(crate) fn init_terminal() -> Result<TerminalSession> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    if let Err(error) = execute!(stdout, EnterAlternateScreen, EnableMouseCapture) {
        cleanup_terminal();
        return Err(error.into());
    }

    match Terminal::new(CrosstermBackend::new(stdout)) {
        Ok(terminal) => Ok(TerminalSession { terminal }),
        Err(error) => {
            cleanup_terminal();
            Err(error.into())
        }
    }
}

pub(crate) struct TerminalSession {
    terminal: TuiTerminal,
}

impl Deref for TerminalSession {
    type Target = TuiTerminal;

    fn deref(&self) -> &Self::Target {
        &self.terminal
    }
}

impl DerefMut for TerminalSession {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.terminal
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        cleanup_terminal();
    }
}

fn cleanup_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), DisableMouseCapture, LeaveAlternateScreen);
}
