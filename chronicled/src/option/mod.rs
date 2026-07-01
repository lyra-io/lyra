use crate::error::unit_error::UnitError;
use crate::error::unit_error::UnitError::Codec;
use crate::option::unit_options::UnitOptions;
use std::fs;

pub mod unit_options;

impl TryInto<UnitOptions> for String {
    type Error = UnitError;

    fn try_into(self) -> Result<UnitOptions, Self::Error> {
        match fs::read_to_string(self) {
            Ok(file_str) => match toml::from_str::<UnitOptions>(&file_str) {
                Ok(options) => Ok(options),
                Err(err) => Err(Codec(err.to_string())),
            },
            Err(err) => Err(Codec(err.to_string())),
        }
    }
}
