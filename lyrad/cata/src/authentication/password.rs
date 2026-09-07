use datafusion_postgres::pgwire::api::auth::sasl::scram::{
    SCRAM_ITERATIONS, gen_salted_password, random_nonce,
};
use meta::proto::pb_catalog::PasswordCredential;

pub(crate) fn make_password_credential(password: &str) -> PasswordCredential {
    let salt = random_nonce().into_bytes();
    PasswordCredential {
        salted_password: gen_salted_password(password, &salt, SCRAM_ITERATIONS).into(),
        salt: salt.into(),
        iterations: SCRAM_ITERATIONS as u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_scram_salted_password_material() {
        let first = make_password_credential("s3cr3t");
        let second = make_password_credential("s3cr3t");

        assert_eq!(first.iterations, SCRAM_ITERATIONS as u32);
        assert_eq!(
            first.salted_password.as_ref(),
            gen_salted_password("s3cr3t", &first.salt, SCRAM_ITERATIONS)
        );
        assert_ne!(first.salted_password.as_ref(), b"s3cr3t");
        assert_ne!(first.salt, second.salt);
    }
}
