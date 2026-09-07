use super::SecretName;
use meta::proto::pb_catalog::Scram;
use std::fmt;

#[derive(Clone, PartialEq, Eq)]
pub enum Value {
    Null,
    Literal(String),
    Secret(SecretName),
    Scram(Scram),
}

impl fmt::Debug for Value {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => formatter.write_str("Null"),
            Self::Literal(_) => formatter.write_str("Literal([REDACTED])"),
            Self::Secret(secret) => formatter.debug_tuple("Secret").field(secret).finish(),
            Self::Scram(_) => formatter.write_str("Scram([REDACTED])"),
        }
    }
}
