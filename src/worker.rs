use crate::{
    error::{CollectedError, CollectedResult},
    peer_connection::Message,
    FileStatus,
};
use rand::{
    prelude::{IteratorRandom, StdRng},
    SeedableRng,
};
use sha1::{Digest, Sha1};
use std::{
    fmt,
    future::Future,
    io::SeekFrom,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    fs::File,
    io::{self, AsyncSeekExt, AsyncWriteExt},
    sync::{mpsc, watch},
    time::Instant,
};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::{debug, error, error_span, info, warn, Instrument};

enum WorkerStreamMessage {
    ReqData(RequestData),
    ReqWork(RequestWork),
    Data(DataBlock),
    Terminate,
}

pub fn get_worker_task(
    mut file_data: FileData,
    mut request_data_receiver: mpsc::UnboundedReceiver<RequestData>,
    mut request_work_receiver: mpsc::UnboundedReceiver<RequestWork>,
    mut data_receiver: mpsc::UnboundedReceiver<DataBlock>,
    pieces_bitfield_sender: watch::Sender<PiecesBitfield>,
    file_status_sender: watch::Sender<FileStatus>,
    downloaded_bytes: Arc<AtomicUsize>,
) -> impl Future<Output = ()> {
    // convert channels to streams
    let request_data_receiver = Box::pin(async_stream::stream! {
        while let Some(item) = request_data_receiver.recv().await {
            yield WorkerStreamMessage::ReqData(item);
        }
    }) as Pin<Box<dyn Stream<Item = WorkerStreamMessage> + Send>>;
    let request_work_receiver = Box::pin(async_stream::stream! {
        while let Some(item) = request_work_receiver.recv().await {
            yield WorkerStreamMessage::ReqWork(item);
        }
    }) as Pin<Box<dyn Stream<Item = WorkerStreamMessage> + Send>>;
    let data_receiver = Box::pin(async_stream::stream! {
        while let Some(item) = data_receiver.recv().await {
            yield WorkerStreamMessage::Data(item);
        }
    }) as Pin<Box<dyn Stream<Item = WorkerStreamMessage> + Send>>;
    let ctrl_c_recv = Box::pin(async_stream::stream! {
        while let Ok(_) = tokio::signal::ctrl_c().await {
            yield WorkerStreamMessage::Terminate;
        }
    }) as Pin<Box<dyn Stream<Item = WorkerStreamMessage> + Send>>;

    let mut stream_map = StreamMap::new();
    stream_map.insert("request_data", request_data_receiver);
    stream_map.insert("request_work", request_work_receiver);
    stream_map.insert("data", data_receiver);
    stream_map.insert("ctrl_c", ctrl_c_recv);

    async move {
        let mut rng: StdRng = SeedableRng::from_entropy();
        while let Some((key, value)) = stream_map.next().await {
            debug!(stream_message_type = key);
            match value {
                // peer task requesting data
                WorkerStreamMessage::ReqData(request_data) => {
                    let msg = if let Some(piece) = file_data.pieces.get(request_data.piece_index) {
                        if let Ok(bytes) = piece.get_block_bytes(
                            request_data.block_begin_offset,
                            request_data.block_length,
                        ) {
                            Some(DataBlock {
                                piece_index: request_data.piece_index,
                                block_begin_offset: request_data.block_begin_offset,
                                block: bytes,
                            })
                        } else {
                            warn!("Piece not yet complete: {}", request_data.piece_index);
                            None
                        }
                    } else {
                        warn!("Piece index out of bounds: {}", request_data.piece_index);
                        None
                    };
                    // TODO: clean up fix for logging
                    if msg.is_some() {
                        debug!(branch = "request block data", received = ?request_data, sending = %msg.as_ref().unwrap());
                    } else {
                        debug!(branch = "request block data", received = ?request_data, sending = ?msg);
                    }
                    if request_data.sender.send(msg).is_err() {
                        error!("Couldn't send back request for block data");
                    };
                }
                // peer task requesting work
                WorkerStreamMessage::ReqWork(request_work) => {
                    // choose an undownloaded block randomly from a random piece
                    let work_block = file_data.get_undownloaded_block(
                        &request_work.peer_pieces,
                        &request_work.current_work,
                        &mut rng
                    );
                    // None means no work for the peer worker
                    debug!(branch = "request work piece", received = %request_work, sending = ?work_block);
                    if request_work.sender.send(work_block).is_err() {
                        error!("Couldn't send back response to work request");
                    }
                }
                // peer task sending block data
                WorkerStreamMessage::Data(block_data) => {
                    // fill block
                    if let Some(piece) = file_data.pieces.get_mut(block_data.piece_index) {
                        if piece.is_complete() {
                            continue
                        }
                        match piece.fill_block(block_data.block_begin_offset, &block_data.block) {
                            Ok(_) => {
                                info!(
                                    "Piece [{}] block filled with {} bytes at offset {}",
                                    block_data.piece_index,
                                    block_data.block.len(),
                                    block_data.block_begin_offset
                                );
                                // if piece is completed by this block, do hash check and inform peer workers
                                if let Ok(valid_piece) =
                                    piece.check_hash(&file_data.hashes[block_data.piece_index])
                                {
                                    if valid_piece {
                                        // update downloaded_bytes stat
                                        // TODO: can make more granular by doing it per block, but have to handle case of discarding invalid pieces
                                        downloaded_bytes
                                            .fetch_add(piece.bytes.len(), Ordering::SeqCst);

                                        // TODO: proper error handling -> retry mechanism?
                                        file_data
                                            .write_piece_to_file(block_data.piece_index)
                                            .await
                                            .unwrap();

                                        let (pieces_complete, total_pieces) =
                                            file_data.get_completion_status();
                                        info!(
                                            "Piece [{}] complete; {}/{}",
                                            block_data.piece_index, pieces_complete, total_pieces
                                        );
                                        if let Err(err) =
                                            pieces_bitfield_sender.send(file_data.get_bitfield())
                                        {
                                            error!("Couldn't send updated bitfield: {}", err);
                                        };

                                        if file_data.is_file_complete() {
                                            info!("File download completed");
                                            if let Err(err) =
                                                file_status_sender.send(FileStatus::Complete)
                                            {
                                                error!(
                                                    "Couldn't send file completion event: {}",
                                                    err
                                                );
                                            };
                                        }
                                    } else {
                                        error!(
                                            "Invalid piece hash, discarding: {}",
                                            block_data.piece_index
                                        );
                                        piece.discard_bytes();
                                    }
                                }
                            }
                            Err(e) => error!("Couldn't fill block with data: {}", e),
                        }
                    } else {
                        error!("Piece index out of bounds: {}", block_data.piece_index);
                    }
                }
                WorkerStreamMessage::Terminate => {
                    info!("SIGINT received, terminating");
                    break;
                }
            }
        }
    }
    .instrument(error_span!("Worker"))
}

#[derive(Debug)]
pub struct FileData {
    pieces: Vec<Piece>,
    piece_length: usize, // not necessarily for last piece
    hashes: Vec<[u8; 20]>,
    file: File,
}

impl FileData {
    pub async fn new(
        file_path: &str,
        pieces_count: usize,
        file_length: usize,
        piece_length: usize,
        hashes: &[u8],
    ) -> io::Result<FileData> {
        let pieces: Vec<Piece> = (0..pieces_count)
            .map(|piece_index| {
                Piece::new(
                    if piece_index == pieces_count - 1 && file_length % piece_length != 0 {
                        // last piece length adjustment
                        file_length % piece_length as usize
                    } else {
                        piece_length
                    },
                    Message::BLOCK_SIZE_BYTES as usize,
                )
            })
            .collect();

        let hashes: Vec<[u8; 20]> = hashes
            .chunks(20)
            .map(|s| {
                let mut hash: [u8; 20] = Default::default();
                hash.copy_from_slice(s);
                hash
            })
            .collect();

        let file = File::create(&file_path).await?;
        file.set_len(file_length as u64).await?;

        Ok(FileData {
            pieces,
            piece_length,
            hashes,
            file,
        })
    }

    pub fn get_bitfield(&self) -> PiecesBitfield {
        let bitfield = self
            .pieces
            .iter()
            .map(|piece| piece.is_complete())
            .collect();
        PiecesBitfield { bitfield }
    }

    fn get_undownloaded_block(
        &self,
        peer_bitfield: &PiecesBitfield,
        current_work: &[WorkBlock],
        rng: &mut StdRng,
    ) -> Option<WorkBlock> {
        self.pieces
            .iter()
            .zip(peer_bitfield.bitfield.iter())
            .enumerate()
            .filter(|(_, (piece, &peer_has_piece))| !piece.is_complete() && peer_has_piece)
            .flat_map(|(index, (piece, _))| piece.request_undownloaded_blocks(index as u32))
            .filter(|work| !current_work.contains(work))
            .choose(rng)
    }

    fn is_file_complete(&self) -> bool {
        // no integrity check since assume if piece is complete, means piece itself is valid
        self.pieces.iter().all(|piece| piece.is_complete())
    }

    async fn write_piece_to_file(&mut self, piece_index: usize) -> CollectedResult<()> {
        match self.pieces.get(piece_index) {
            Some(piece) => {
                self.file
                    .seek(SeekFrom::Start((piece_index * self.piece_length) as u64))
                    .await?;
                self.file.write_all(&piece.bytes).await?;
                Ok(())
            }
            None => Err("Piece index out of bounds".into()),
        }
    }

    // (total complete pieces count, total pieces count)
    fn get_completion_status(&self) -> (usize, usize) {
        let total_complete_pieces = self.pieces.iter().filter(|p| p.is_complete()).count();
        let total_pieces_count = self.pieces.len();
        (total_complete_pieces, total_pieces_count)
    }
}

#[derive(Debug)]
struct Piece {
    bytes: Vec<u8>,
    block_downloaded: Vec<bool>, // true if block at index has been downloaded, split by block boundaries
    already_requested: Vec<Option<Instant>>, // set to when block was already requested (to avoid doubling up)
    // TODO: get rid of this, it's constant
    max_block_length: usize, // untruncated
}

impl Piece {
    fn new(piece_length: usize, max_block_length: usize) -> Piece {
        // (A + (B-1)) / B -> ceiling division
        // TODO: technically could overflow though?
        let no_of_blocks = (piece_length + max_block_length - 1) / max_block_length;
        Piece {
            bytes: vec![0; piece_length],
            block_downloaded: vec![false; no_of_blocks],
            already_requested: vec![None; no_of_blocks],
            max_block_length,
        }
    }

    fn block_length(&self, block_index: usize) -> CollectedResult<usize> {
        if block_index >= self.block_downloaded.len() {
            Err(CollectedError::GenericError(format!(
                "Block index out of bounds: {}",
                block_index
            )))
        } else if block_index == self.block_downloaded.len() - 1
            && self.bytes.len() % self.max_block_length != 0
        {
            // calculate last block length in case truncated
            Ok(self.bytes.len() % self.max_block_length)
        } else {
            Ok(self.max_block_length)
        }
    }

    // TODO: support checking of the Instant, and easier setting
    fn request_undownloaded_blocks(&self, piece_index: u32) -> Vec<WorkBlock> {
        self.block_downloaded
            .iter()
            .enumerate()
            .filter_map(|(block_index, &block_downloaded)| {
                if !block_downloaded {
                    Some(block_index)
                } else {
                    None
                }
            })
            // safe unwrap since know index will be within bounds always
            .map(|block_index| WorkBlock {
                piece_index,
                block_begin_offset: (block_index * self.max_block_length) as u32,
                // block_length() does bounds check
                block_length: self.block_length(block_index).unwrap() as u32,
            })
            .collect()
    }

    // TODO: implement this
    fn set_block_instant(&mut self, block_index: usize) {}

    fn fill_block(&mut self, block_begin_offset: usize, block: &[u8]) -> CollectedResult<()> {
        let block_index = block_begin_offset / self.max_block_length;
        let block_length = self.block_length(block_index)?;
        if block_begin_offset % self.max_block_length != 0 {
            // begin index not aligned to block boundaries
            Err(CollectedError::GenericError(format!(
                        "Byte offset within piece: {}, not aligned to block boundry where block size is: {}",
                        block_begin_offset, self.max_block_length
                    )))
        } else if block_length != block.len() {
            // block lengths don't match
            Err(CollectedError::GenericError(format!(
                "Block lengths don't match, expected length: {}, received length: {}",
                block_length,
                block.len()
            )))
        } else {
            {
                let start = block_begin_offset as usize;
                let end = start + block_length;
                self.bytes[start..end].copy_from_slice(block);
            }
            self.block_downloaded[block_index] = true;
            Ok(())
        }
    }

    fn is_complete(&self) -> bool {
        self.block_downloaded.iter().all(|&b| b)
    }

    fn get_block_bytes(
        &self,
        block_begin_offset: usize,
        block_length: usize,
    ) -> CollectedResult<Vec<u8>> {
        if self.is_complete() {
            // retrieves specific block within piece, defined by offset and length
            Ok(self.bytes[block_begin_offset..(block_begin_offset + block_length)].to_vec())
        } else {
            Err("Piece not yet complete".into())
        }
    }

    fn check_hash(&self, expected_hash: &[u8; 20]) -> CollectedResult<bool> {
        if self.is_complete() {
            // calculate SHA1 and compare
            let mut hasher = Sha1::new();
            hasher.update(&self.bytes);
            let actual_hash = hasher.finalize().to_vec();
            Ok(actual_hash == expected_hash)
        } else {
            Err("Piece not yet complete".into())
        }
    }

    fn discard_bytes(&mut self) {
        self.block_downloaded.iter_mut().for_each(|b| *b = false);
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PiecesBitfield {
    bitfield: Vec<bool>,
}

impl fmt::Display for PiecesBitfield {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PiecesBitfield {{ bitfield.len(): {} }}",
            self.bitfield.len()
        )
    }
}

impl PiecesBitfield {
    pub fn new(pieces_count: usize) -> PiecesBitfield {
        PiecesBitfield {
            bitfield: vec![false; pieces_count],
        }
    }

    pub fn from_bytes(bytes: &[u8], pieces_count: usize) -> CollectedResult<PiecesBitfield> {
        let mut bitfield: Vec<bool> = bytes
            .iter()
            .flat_map(|byte| (0..8).map(move |index| ((0b1000_0000 >> index) & byte) != 0))
            .collect();
        if bitfield.len() < pieces_count {
            Err(format!(
                "Pieces count mismatch, expected: {}, but got: {}",
                pieces_count,
                bitfield.len()
            )
            .into())
        } else if bitfield.len() == pieces_count {
            Ok(PiecesBitfield { bitfield })
        } else if bitfield.split_off(pieces_count + 1).into_iter().any(|b| b) {
            Err("Extra bits were set".into())
        } else {
            Ok(PiecesBitfield { bitfield })
        }
    }

    // returns indices that have changed (false -> true)
    pub fn merge(&mut self, other: &PiecesBitfield) -> Vec<usize> {
        let delta_indices: Vec<usize> = self
            .bitfield
            .iter()
            .zip(other.bitfield.iter())
            .enumerate()
            .filter(|(_, (&old_flag, &new_flag))| !old_flag && new_flag)
            .map(|(index, _)| index)
            .collect();
        // update current bool field
        delta_indices
            .iter()
            .for_each(|&index| self.bitfield[index] = true);
        delta_indices
    }

    pub fn set_has_piece(&mut self, piece_index: usize) -> CollectedResult<()> {
        if let Some(b) = self.bitfield.get_mut(piece_index) {
            *b = true;
            Ok(())
        } else {
            Err(format!("Index out of bounds: {piece_index}").into())
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        // high bit in the first byte corresponds to piece index 0
        self.bitfield
            .chunks(8)
            .map(|bools| {
                bools
                    .iter()
                    .enumerate()
                    .map(|(index, &flag)| {
                        if flag {
                            0b1000_0000 >> index
                        } else {
                            0b0000_0000
                        }
                    })
                    .fold(0b0000_0000, |a, b| a | b)
            })
            .collect()
    }

    pub fn at_least_one_piece_present(&self) -> bool {
        self.bitfield.iter().any(|&flag| flag)
    }

    pub fn pieces_count(&self) -> usize {
        self.bitfield.len()
    }
}

#[derive(Debug)]
pub struct RequestWork {
    peer_pieces: PiecesBitfield,
    sender: mpsc::UnboundedSender<Option<WorkBlock>>,
    current_work: Vec<WorkBlock>, // which work the peer task already has, to avoid duplicates
}

impl RequestWork {
    pub fn new(
        peer_pieces: &PiecesBitfield,
        sender: &mpsc::UnboundedSender<Option<WorkBlock>>,
        current_work: &[WorkBlock],
    ) -> RequestWork {
        RequestWork {
            peer_pieces: peer_pieces.clone(),
            sender: sender.clone(),
            current_work: current_work.to_vec(),
        }
    }
}

impl fmt::Display for RequestWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RequestWork {{ peer_pieces: {}, sender: {:?} }}",
            self.peer_pieces, self.sender
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WorkBlock {
    pub piece_index: u32,
    pub block_begin_offset: u32,
    pub block_length: u32,
}

impl WorkBlock {
    pub fn to_request_message(self) -> Message {
        Message::Request {
            piece_index: self.piece_index,
            block_begin_offset: self.block_begin_offset,
            block_length: self.block_length,
        }
    }
}

#[derive(Debug)]
pub struct RequestData {
    // TODO: first 3 fields duplicated with WorkBlock?
    pub piece_index: usize,
    pub block_begin_offset: usize,
    pub block_length: usize,
    pub sender: mpsc::UnboundedSender<Option<DataBlock>>,
}

#[derive(Debug)]
pub struct DataBlock {
    pub piece_index: usize,
    pub block_begin_offset: usize,
    pub block: Vec<u8>,
}

impl fmt::Display for DataBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DataBlock {{ piece_index: {}, block_begin_offset: {}, block.len(): {} }}",
            self.piece_index,
            self.block_begin_offset,
            self.block.len()
        )
    }
}

impl DataBlock {
    pub fn as_work_block(&self) -> WorkBlock {
        WorkBlock {
            piece_index: self.piece_index as u32,
            block_begin_offset: self.block_begin_offset as u32,
            block_length: self.block.len() as u32, // TODO: unsure about this
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pieces_bitfield() {
        // regular
        let _expected = PiecesBitfield {
            bitfield: vec![
                true, false, true, false, false, false, true, false, true, false, true,
            ],
        };
        assert!(matches!(
            PiecesBitfield::from_bytes(&[0b1010_0010, 0b1010_0000], 11),
            Ok(_expected)
        ));
        // not enough bits
        assert!(PiecesBitfield::from_bytes(&[0b1010_0010], 11).is_err());
        // extra bits set
        assert!(PiecesBitfield::from_bytes(&[0b1010_0010, 0b1010_0010], 10).is_err());
        // edge condition
        let _expected = PiecesBitfield {
            bitfield: vec![true, false, true, false, false, false, true, false],
        };
        assert!(matches!(
            PiecesBitfield::from_bytes(&[0b1010_0010], 8),
            Ok(_expected)
        ));
    }
}
