use crate::error::{CollectedError, CollectedResult};
use std::{collections::BTreeMap, slice};

// TODO: consider using regex instead? https://web.archive.org/web/20200105114449/https://effbot.org/zone/bencode.htm

type IntSize = i64;

#[derive(Clone, Debug)]
pub enum BencodeValue {
    Integer(IntSize),
    String(String),
    List(Vec<BencodeValue>),
    Dictionary(BTreeMap<String, BencodeValue>),
}

impl BencodeValue {
    pub fn encode(&self) -> String {
        match self {
            BencodeValue::Integer(value) => format!("i{}e", value),
            BencodeValue::String(value) => format!("{}:{}", value.chars().count(), value), // LEN() IS INCORRECT
            BencodeValue::List(value) => {
                format!(
                    "l{}e",
                    value
                        .iter()
                        .map(|item| item.encode())
                        .fold(String::new(), |mut a, b| {
                            a.push_str(&b);
                            a
                        })
                )
            }
            BencodeValue::Dictionary(value) => {
                format!(
                    "d{}e",
                    value
                        .iter() // CASE SENSITIVE ORDER!
                        .map(|(key, item)| format!(
                            "{}{}",
                            BencodeValue::String(key.to_string()).encode(),
                            item.encode()
                        ))
                        .fold(String::new(), |mut a, b| {
                            a.push_str(&b);
                            a
                        })
                )
            }
        }
    }

    pub fn get_integer(&self) -> CollectedResult<&IntSize> {
        match self {
            BencodeValue::Integer(i) => Ok(i),
            _ => Err(CollectedError::BencodeCastError(
                "Not an integer".to_string(),
            )),
        }
    }

    pub fn get_string(&self) -> CollectedResult<&String> {
        match self {
            BencodeValue::String(s) => Ok(s),
            _ => Err(CollectedError::BencodeCastError("Not a string".to_string())),
        }
    }

    pub fn get_list(&self) -> CollectedResult<&Vec<BencodeValue>> {
        match self {
            BencodeValue::List(l) => Ok(l),
            _ => Err(CollectedError::BencodeCastError("Not a list".to_string())),
        }
    }

    pub fn get_dictionary(&self) -> CollectedResult<&BTreeMap<String, BencodeValue>> {
        match self {
            BencodeValue::Dictionary(d) => Ok(d),
            _ => Err(CollectedError::BencodeCastError(
                "Not a dictionary".to_string(),
            )),
        }
    }
}

pub fn parse(mut iter: slice::Iter<u8>) -> CollectedResult<BencodeValue> {
    let val = match *iter
        .next()
        .ok_or_else(|| CollectedError::BencodeParseError("Iterator error".to_string()))?
        as char
    {
        'i' => parse_integer(&mut iter),
        'd' => parse_dictionary(&mut iter),
        'l' => parse_list(&mut iter),
        c => parse_string_with_initial(&mut iter, c),
    }?;

    Ok(val)
}

fn parse_integer(iter: &mut slice::Iter<u8>) -> CollectedResult<BencodeValue> {
    // ASSUMES INITIAL I IS OMITTED!
    let mut c = *iter.next().ok_or("Iterator error")? as char;

    let mut s = String::new();

    while c != 'e' {
        s.push(c);
        c = *iter.next().ok_or("Iterator error")? as char;
    }

    Ok(BencodeValue::Integer(s.parse::<IntSize>()?))
}

fn parse_dictionary(iter: &mut slice::Iter<u8>) -> CollectedResult<BencodeValue> {
    // ASSUMES INITIAL D IS OMITTED!
    let mut dictionary: BTreeMap<String, BencodeValue> = BTreeMap::new();

    let mut c = *iter.next().ok_or("Iterator error")? as char;

    while c != 'e' {
        let key = match parse_string_with_initial(iter, c)? {
            BencodeValue::String(key_string) => Ok(key_string),
            _ => Err("Non-string key for dictionary"),
        }?;

        c = *iter.next().ok_or("Iterator error")? as char;
        let val = match c {
            'i' => parse_integer(iter),
            'd' => parse_dictionary(iter),
            'l' => parse_list(iter),
            s => parse_string_with_initial(iter, s),
        }?;
        dictionary.insert(key, val);
        c = *iter.next().ok_or("Iterator error")? as char;
    }

    Ok(BencodeValue::Dictionary(dictionary))
}

fn parse_list(iter: &mut slice::Iter<u8>) -> Result<BencodeValue, CollectedError> {
    // ASSUMES INITIAL L IS OMITTED!
    let mut list = vec![];

    let mut c = *iter.next().ok_or("Iterator error")? as char;

    while c != 'e' {
        let val = match c {
            'i' => parse_integer(iter),
            'd' => parse_dictionary(iter),
            'l' => parse_list(iter),
            s => parse_string_with_initial(iter, s),
        }?;
        list.push(val);
        c = *iter.next().ok_or("Iterator error")? as char;
    }

    Ok(BencodeValue::List(list))
}

fn parse_string_with_initial(
    iter: &mut slice::Iter<u8>,
    initial: char,
) -> Result<BencodeValue, CollectedError> {
    let mut c = *iter.next().ok_or("Iterator error")? as char;

    let mut size_str = initial.to_string();

    while c != ':' {
        size_str.push(c);
        c = *iter.next().ok_or("Iterator error")? as char;
    }

    let size = size_str.parse::<u64>()?;

    let mut s = String::new();
    for _ in 0..size {
        s.push(*iter.next().ok_or("Iterator error")? as char);
    }

    Ok(BencodeValue::String(s))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_string_with_initial() {
        let input = "7:Kaladini10e";

        let mut i = input.as_bytes().iter();
        let c = *i.next().unwrap() as char;

        let output = parse_string_with_initial(&mut i, c);

        assert!(matches!(output, Ok(BencodeValue::String(t)) if t == "Kaladin"));
    }

    #[test]
    fn test_integer_positive() {
        let input = "i10e7:Kaladin";

        let mut i = input.as_bytes().iter();
        i.next();

        let output = parse_integer(&mut i);

        assert!(matches!(output, Ok(BencodeValue::Integer(t)) if t == 10));
    }

    #[test]
    fn test_integer_negative() {
        let input = "i-10123e7:Kaladin";

        let mut i = input.as_bytes().iter();
        i.next();

        let output = parse_integer(&mut i);

        assert!(matches!(output, Ok(BencodeValue::Integer(t)) if t == -10123));
    }

    #[test]
    fn test_integer_zero() {
        let input = "i0e7:Kaladin";

        let mut i = input.as_bytes().iter();
        i.next();

        let output = parse_integer(&mut i);

        assert!(matches!(output, Ok(BencodeValue::Integer(t)) if t == 0));
    }

    #[test]
    fn test_list_populated() {
        let input = "li10e7:Kaladinei0e7:Kaladin";

        let mut i = input.as_bytes().iter();
        i.next();

        let output = parse_list(&mut i);

        assert!(matches!(output, Ok(BencodeValue::List(_))));

        let list = match output.unwrap() {
            BencodeValue::List(l) => l,
            _ => unreachable!(),
        };
        assert!(list.len() == 2);
    }

    #[test]
    fn test_list_empty() {
        let input = "lei0e7:Kaladin";

        let mut i = input.as_bytes().iter();
        i.next();

        let output = parse_list(&mut i);

        assert!(matches!(output, Ok(BencodeValue::List(_))));

        let list = match output.unwrap() {
            BencodeValue::List(l) => l,
            _ => unreachable!(),
        };
        assert!(list.is_empty());
    }

    #[test]
    fn test_dictionary() {
        let input = "d7:Kaladini10ee7:Kaladin";

        let mut i = input.as_bytes().iter();
        i.next();

        let output = parse_dictionary(&mut i);

        assert!(matches!(output, Ok(BencodeValue::Dictionary(_))));

        let dict = match output.unwrap() {
            BencodeValue::Dictionary(l) => l,
            _ => unreachable!(),
        };
        assert!(dict.len() == 1);
        assert!(matches!(dict.get("Kaladin"), Some(BencodeValue::Integer(t)) if t == &10));
    }

    #[test]
    fn test_encode_dictionary() {
        let input = "d6:Adolin7:Shallan7:Kaladini10e6:Sadeas4:dead7:dalinar5:Unitee";

        let mut i = input.as_bytes().iter();
        i.next();

        let output = parse_dictionary(&mut i);

        assert!(matches!(output, Ok(BencodeValue::Dictionary(_))));

        let output = output.unwrap();

        assert!(output.encode() == input);
    }
}
