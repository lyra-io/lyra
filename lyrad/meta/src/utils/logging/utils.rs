//! Small utilities shared by stream storage components.

/// Logs a failed cleanup operation and deliberately ignores its error.
///
/// This is intended only for best-effort close and cleanup paths.
#[macro_export]
macro_rules! log_ignore {
    ($stage:expr, $operation:expr) => {{
        match $operation {
            Ok(_) => {}
            Err(error) => {
                tracing::error!(
                    stage = $stage,
                    error = %error,
                    "operation failed and was ignored"
                );
            }
        }
    }};
}

pub use crate::log_ignore;
