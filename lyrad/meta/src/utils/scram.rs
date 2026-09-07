use crate::proto::pb_catalog::{Scram, Value, value};
use ring::pbkdf2::{self, PBKDF2_HMAC_SHA256};
use std::borrow::Cow;
use std::num::NonZeroU32;

pub const SCRAM_ITERATIONS: u32 = 4096;

const SCRAM_SALT_LENGTH: usize = 18;
const SCRAM_SHA_256_OUTPUT_LENGTH: usize = 32;

pub fn make_scram(password: &str) -> Scram {
    let salt = rand::random::<[u8; SCRAM_SALT_LENGTH]>();
    Scram {
        salted_password: derive0(password, &salt, SCRAM_ITERATIONS)
            .expect("the SCRAM iteration count is nonzero")
            .into(),
        salt: salt.to_vec().into(),
        iterations: SCRAM_ITERATIONS,
    }
}

pub fn make_scram_value(password: &str) -> Value {
    Value {
        kind: Some(value::Kind::Scram(make_scram(password))),
    }
}

pub fn as_scram(value: &Value) -> Option<&Scram> {
    match value.kind.as_ref()? {
        value::Kind::Scram(scram) => Some(scram),
        value::Kind::Literal(_) | value::Kind::Secret(_) => None,
    }
}

pub fn is_valid_scram(scram: &Scram) -> bool {
    scram.iterations == SCRAM_ITERATIONS
        && scram.salted_password.len() == SCRAM_SHA_256_OUTPUT_LENGTH
        && !scram.salt.is_empty()
}

pub fn verify_scram(password: &str, scram: &Scram) -> bool {
    let Some(iterations) = NonZeroU32::new(scram.iterations) else {
        return false;
    };
    if scram.salted_password.len() != SCRAM_SHA_256_OUTPUT_LENGTH {
        return false;
    }
    let normalized = normalize0(password);
    pbkdf2::verify(
        PBKDF2_HMAC_SHA256,
        iterations,
        &scram.salt,
        normalized.as_bytes(),
        &scram.salted_password,
    )
    .is_ok()
}

fn derive0(password: &str, salt: &[u8], iterations: u32) -> Option<Vec<u8>> {
    let iterations = NonZeroU32::new(iterations)?;
    let normalized = normalize0(password);
    let mut salted_password = vec![0; SCRAM_SHA_256_OUTPUT_LENGTH];
    pbkdf2::derive(
        PBKDF2_HMAC_SHA256,
        iterations,
        salt,
        normalized.as_bytes(),
        &mut salted_password,
    );
    Some(salted_password)
}

fn normalize0(password: &str) -> Cow<'_, str> {
    stringprep::saslprep(password).unwrap_or(Cow::Borrowed(password))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_scram_salted_password_material() {
        let first = make_scram("s3cr3t");
        let second = make_scram("s3cr3t");

        assert_eq!(first.iterations, SCRAM_ITERATIONS);
        assert!(verify_scram("s3cr3t", &first));
        assert!(!verify_scram("wrong", &first));
        assert_ne!(first.salted_password.as_ref(), b"s3cr3t");
        assert_ne!(first.salt, second.salt);
    }
}
