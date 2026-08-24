use tracing::{error, info};

pub async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.ok();
    }
}

pub fn write_pid_file(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pid = std::process::id();
    std::fs::write(path, pid.to_string())
        .map_err(|e| format!("failed to write PID file '{}': {}", path, e))?;
    info!(pid = pid, path = path, "pid file written");
    Ok(())
}

pub fn remove_pid_file(path: &str) {
    if let Err(e) = std::fs::remove_file(path) {
        error!("failed to remove PID file '{}': {}", path, e);
    }
}

pub fn read_pid_file(path: &str) -> Result<i32, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read PID file '{}': {}", path, e))?;
    let pid: i32 = contents
        .trim()
        .parse()
        .map_err(|e| format!("invalid PID in '{}': {}", path, e))?;
    Ok(pid)
}

pub fn send_sigterm(pid: i32) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    {
        let ret = unsafe { libc::kill(pid, libc::SIGTERM) };
        if ret != 0 {
            return Err(format!(
                "failed to send SIGTERM to pid {}: {}",
                pid,
                std::io::Error::last_os_error()
            )
            .into());
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        Err("stop command is only supported on Unix systems".into())
    }
}
