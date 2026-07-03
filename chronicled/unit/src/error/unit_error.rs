use thiserror::Error;

#[derive(Error, Debug)]
pub enum UnitError {
    #[error("Resource is unavailable: {0}")]
    Unavailable(String),
    #[error("Transport error: {0}")]
    Transport(String),
    #[error("Codec error: {0}")]
    Codec(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Task error: {0}")]
    TaskError(String),
    #[error("WAL error")]
    Wal,
    #[error("Invalid term: current={current}, requested={requested}")]
    InvalidTerm { current: i64, requested: i64 },
    #[error("Fenced: timeline {timeline_id} at term {term}")]
    Fenced { timeline_id: i64, term: i64 },
}
