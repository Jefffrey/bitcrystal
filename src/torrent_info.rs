use crate::{
    bencode,
    error::{CollectedError, CollectedResult},
};
use sha1::{Digest, Sha1};

// TODO: support multi-file
#[derive(Debug)]
pub struct TorrentInfo {
    pub announce_list: Vec<Vec<String>>, // alternate trackers, divided into groups
    pub announce: String,                // tracker URL
    pub name: String,                    // filename
    pub length: u64,                     // file length in bytes
    pub piece_length: u64,               // no. of bytes in each piece
    pub pieces: Vec<u8>,                 // concatenated 20-byte SHA1 hashes for each piece
    pub info_hash: [u8; 20],             // SHA1 hash of Bencode encoded info block (info key value)
    pub creation_date: Option<u64>,      // seconds since epoch
    pub comment: Option<String>,         // optional comment
    pub created_by: Option<String>,      // origin
}

impl TorrentInfo {
    pub fn new(bencode: bencode::BencodeValue) -> CollectedResult<TorrentInfo> {
        let root = bencode.get_dictionary()?;

        let announce = root
            .get("announce")
            .ok_or_else(|| CollectedError::MissingKeyError("announce".to_string()))?
            .get_string()?
            .to_string();

        let comment = root
            .get("comment")
            .and_then(|v| v.get_string().ok())
            .cloned();

        let created_by = root
            .get("created by")
            .and_then(|v| v.get_string().ok())
            .cloned();

        let creation_date = root
            .get("creation date")
            .and_then(|v| v.get_integer().ok())
            .map(|i| *i as u64);

        let announce_list = root
            .get("announce-list")
            .and_then(|v| v.get_list().ok().cloned())
            .unwrap_or_default()
            .iter()
            .map(|v| {
                v.get_list()
                    .ok()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|s| s.get_string().ok().cloned())
                    .collect()
            })
            .collect::<Vec<Vec<String>>>();

        let info = root
            .get("info")
            .ok_or_else(|| CollectedError::MissingKeyError("info".to_string()))?;

        let info_bencode_block = info.encode();

        let info = info.get_dictionary()?;

        let length = *info
            .get("length")
            .ok_or_else(|| CollectedError::MissingKeyError("length".to_string()))?
            .get_integer()? as u64;

        let name = info
            .get("name")
            .ok_or_else(|| CollectedError::MissingKeyError("name".to_string()))?
            .get_string()?
            .to_string();

        let piece_length = *info
            .get("piece length")
            .ok_or_else(|| CollectedError::MissingKeyError("piece length".to_string()))?
            .get_integer()? as u64;

        let mut hasher = Sha1::new();
        hasher.update(
            info_bencode_block
                .chars()
                .map(|c| c as u8) // since pieces is byte string, NOT UTF-8
                .collect::<Vec<u8>>(),
        );
        let mut info_hash: [u8; 20] = Default::default();
        info_hash.copy_from_slice(&hasher.finalize());

        let pieces = info
            .get("pieces")
            .ok_or_else(|| CollectedError::MissingKeyError("pieces".to_string()))?
            .get_string()?
            .chars()
            .map(|c| c as u8) // since pieces is byte string, NOT UTF-8
            .collect::<Vec<u8>>();

        Ok(TorrentInfo {
            announce_list,
            announce,
            name,
            length,
            piece_length,
            pieces,
            info_hash,
            creation_date,
            comment,
            created_by,
        })
    }
}
