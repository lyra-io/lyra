use crate::metadata::{MetadataError, Result};
use crate::proto::pb_catalog::User;
use crate::utils::scram::{as_scram, is_valid_scram};

pub(crate) fn validate_user(user: &User) -> Result<()> {
    let Some(password) = user.password.as_ref() else {
        return Ok(());
    };
    if as_scram(password).is_some_and(is_valid_scram) {
        return Ok(());
    }
    Err(MetadataError::InvalidUserPassword(user.name.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::pb_catalog::{Scram, Value, value};
    use crate::utils::scram::make_scram_value;

    #[test]
    fn accepts_absent_and_scram_passwords() {
        validate_user(&User {
            name: "without-password".to_string(),
            password: None,
        })
        .unwrap();
        validate_user(&User {
            name: "with-password".to_string(),
            password: Some(make_scram_value("password")),
        })
        .unwrap();
    }

    #[test]
    fn rejects_unresolved_and_invalid_passwords() {
        for password in [
            Value {
                kind: Some(value::Kind::Literal("password".to_string())),
            },
            Value {
                kind: Some(value::Kind::Secret("login_password".to_string())),
            },
            Value {
                kind: Some(value::Kind::Scram(Scram::default())),
            },
            Value::default(),
        ] {
            let error = validate_user(&User {
                name: "alice".to_string(),
                password: Some(password),
            })
            .unwrap_err();

            assert!(matches!(error, MetadataError::InvalidUserPassword(name) if name == "alice"));
        }
    }
}
