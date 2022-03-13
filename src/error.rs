use std::{error::Error, fmt, num::ParseIntError};

pub type CollectedResult<T> = Result<T, CollectedError>;
pub type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Debug)]
pub enum CollectedError {
    IntegerParseError(String),
    GenericError(String),
    BencodeParseError(String),
    BencodeCastError(String),
    MissingKeyError(String),
    MalformedMessageBytes(String),
    StdIoError(std::io::Error),
}

impl fmt::Display for CollectedError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CollectedError::IntegerParseError(ref err)
            | CollectedError::BencodeParseError(ref err)
            | CollectedError::BencodeCastError(ref err)
            | CollectedError::GenericError(ref err)
            | CollectedError::MissingKeyError(ref err)
            | CollectedError::MalformedMessageBytes(ref err) => err.fmt(fmt),
            CollectedError::StdIoError(ref err) => err.fmt(fmt),
        }
    }
}

impl Error for CollectedError {}

impl From<ParseIntError> for CollectedError {
    fn from(err: ParseIntError) -> Self {
        CollectedError::IntegerParseError(err.to_string())
    }
}

impl From<String> for CollectedError {
    fn from(err: String) -> Self {
        CollectedError::GenericError(err)
    }
}

impl From<&'static str> for CollectedError {
    fn from(err: &'static str) -> Self {
        CollectedError::GenericError(err.to_string())
    }
}

impl From<std::io::Error> for CollectedError {
    fn from(err: std::io::Error) -> Self {
        CollectedError::StdIoError(err)
    }
}
