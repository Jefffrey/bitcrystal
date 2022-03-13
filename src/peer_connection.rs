use crate::{
    error::{BoxError, CollectedError, CollectedResult},
    worker::{DataBlock, PiecesBitfield, RequestData, RequestWork, WorkBlock},
    FileStatus,
};
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BytesMut};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt,
    future::Future,
    io::{Error, ErrorKind},
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tokio_util::codec::{Decoder, FramedRead};
use tracing::{debug, error, error_span, info, warn, Instrument};

const REQUEST_BACKLOG_SIZE: usize = 5;
const MIN_BLOCK_BYTES_LENGTH: u32 = 4 * 1024;
const MAX_BLOCK_BYTES_LENGTH: u32 = 128 * 1024;
const RETRY_COUNT: u32 = 5;
const RETRY_TIMEOUT_DURATION: Duration = Duration::from_secs(30);

pub fn get_listener_task(
    local_port: u16,
    max_passive_concurrent_peers: usize,
    client_id: String,
    info_hash: [u8; 20],
    pieces_bitfield_receiver: watch::Receiver<PiecesBitfield>,
    file_status_receiver: watch::Receiver<FileStatus>,
    request_work_sender: mpsc::UnboundedSender<RequestWork>,
    request_data_sender: mpsc::UnboundedSender<RequestData>,
    data_sender: mpsc::UnboundedSender<DataBlock>,
    uploaded_bytes: Arc<AtomicUsize>,
) -> impl Future<Output = Result<(), BoxError>> {
    let (task_completion_sender, mut task_completion_receiver) =
        mpsc::unbounded_channel::<SocketAddr>();
    async move {
        let mut current_connections: usize = 0;
        let listener = TcpListener::bind(format!("0.0.0.0:{}", local_port)).await?;
        let mut tasks = HashMap::new();
        loop {
            tokio::select! {
                // listening for incoming connections
                Ok((stream, addr)) = listener.accept(), if current_connections < max_passive_concurrent_peers => {
                    current_connections += 1;
                    let task = tokio::task::Builder::new()
                        .name(&format!("Passive {}", addr))
                        .spawn(get_peer_task(
                            addr,
                            client_id.clone(),
                            info_hash,
                            pieces_bitfield_receiver.clone(),
                            file_status_receiver.clone(),
                            request_work_sender.clone(),
                            request_data_sender.clone(),
                            data_sender.clone(),
                            task_completion_sender.clone(),
                            uploaded_bytes.clone(),
                            Some(stream),
                        ));
                    tasks.insert(addr, task);
                },
                Some(addr) = task_completion_receiver.recv() => {
                    current_connections -= 1;
                    tasks.remove(&addr);
                },
                // terminate gracefully
                Ok(_) = tokio::signal::ctrl_c() => {
                    info!("SIGINT received, terminating");
                    // wait for child tasks to finish
                    futures::future::join_all(tasks.into_values()).await;
                    break;
                },
                else => continue,
            }
        }
        Ok::<(), BoxError>(())
    }
    .instrument(error_span!("Listener"))
}

enum PeerStreamMessage {
    ReqWork(Option<WorkBlock>),
    ReqData(Option<DataBlock>),
    ReqTimeout(WorkBlock),
    BitfieldUpdate(PiecesBitfield),
    FileStatusUpdate(FileStatus),
    TcpRead(Result<Message, Error>),
    Terminate,
}

pub fn get_peer_task(
    peer: SocketAddr,
    client_id: String,
    info_hash: [u8; 20],
    pieces_bitfield_receiver: watch::Receiver<PiecesBitfield>,
    file_status_receiver: watch::Receiver<FileStatus>,
    request_work_sender: mpsc::UnboundedSender<RequestWork>,
    request_data_sender: mpsc::UnboundedSender<RequestData>,
    data_sender: mpsc::UnboundedSender<DataBlock>,
    peer_task_completion_sender: mpsc::UnboundedSender<SocketAddr>,
    uploaded_bytes: Arc<AtomicUsize>,
    tcp_stream: Option<TcpStream>,
) -> impl Future<Output = Result<(), BoxError>> {
    async move {
        debug!("Peer task spawned");
        let result = async move {
            let mut processor = PeerTaskProcessor::new(
                pieces_bitfield_receiver,
                file_status_receiver,
                request_work_sender,
                request_data_sender,
                data_sender,
                uploaded_bytes,
            )?;
            processor
                .main_loop(peer, client_id, info_hash, tcp_stream)
                .await?;
            Ok::<(), BoxError>(())
        }
        .await;
        debug!("Peer task complete: {:?}", result);
        if let Err(e) = peer_task_completion_sender.send(peer) {
            error!("Couldn't send task completion: {}", e);
        }
        result
    }
    .instrument(error_span!("Peer", address = %peer))
}

struct PeerTaskProcessor {
    our_bitfield: PiecesBitfield,
    am_choking: bool,      // are we choking peer
    am_interested: bool,   // are we interested in peer
    peer_choking: bool,    // is peer choking us
    peer_interested: bool, // is peer interested in us
    peer_bitfield: PiecesBitfield,
    request_cache: Vec<WorkBlock>,
    cancels: HashSet<WorkBlock>,
    stream_map: StreamMap<&'static str, Pin<Box<dyn Stream<Item = PeerStreamMessage> + Send>>>,
    work_channel_sender: mpsc::UnboundedSender<Option<WorkBlock>>,
    data_channel_sender: mpsc::UnboundedSender<Option<DataBlock>>,
    delay_map: HashMap<WorkBlock, (JoinHandle<()>, u32)>,
    delay_sender: mpsc::UnboundedSender<WorkBlock>,
    request_work_sender: mpsc::UnboundedSender<RequestWork>,
    request_data_sender: mpsc::UnboundedSender<RequestData>,
    data_sender: mpsc::UnboundedSender<DataBlock>,
    uploaded_bytes: Arc<AtomicUsize>,
    continue_main_loop: bool,
    file_complete: bool,
}

impl PeerTaskProcessor {
    fn new(
        mut pieces_bitfield_receiver: watch::Receiver<PiecesBitfield>,
        mut file_status_receiver: watch::Receiver<FileStatus>,
        request_work_sender: mpsc::UnboundedSender<RequestWork>,
        request_data_sender: mpsc::UnboundedSender<RequestData>,
        data_sender: mpsc::UnboundedSender<DataBlock>,
        uploaded_bytes: Arc<AtomicUsize>,
    ) -> Result<PeerTaskProcessor, BoxError> {
        let our_bitfield = pieces_bitfield_receiver.borrow_and_update().clone();
        let peer_bitfield = PiecesBitfield::new(our_bitfield.pieces_count());

        // first try to get from request cache, else request from worker (which will then populate this cache if delay map is full)
        let request_cache = Vec::with_capacity(REQUEST_BACKLOG_SIZE);

        // before sending Piece messages, consult this to see if peer has cancelled the request in the meantime
        let cancels = HashSet::new();

        // give senders to worker, with receiver to get the response
        let (work_channel_sender, mut work_channel_receiver) =
            mpsc::unbounded_channel::<Option<WorkBlock>>();
        let (data_channel_sender, mut data_channel_receiver) =
            mpsc::unbounded_channel::<Option<DataBlock>>();

        // (sleep task, retry count)
        let delay_map = HashMap::with_capacity(REQUEST_BACKLOG_SIZE);
        let (delay_sender, mut delay_receiver) = mpsc::unbounded_channel::<WorkBlock>();

        // readying the stream map
        let request_work_receiver = Box::pin(async_stream::stream! {
            while let Some(item) = work_channel_receiver.recv().await {
                yield PeerStreamMessage::ReqWork(item);
            }
        })
            as Pin<Box<dyn Stream<Item = PeerStreamMessage> + Send>>;
        let request_data_receiver = Box::pin(async_stream::stream! {
            while let Some(item) = data_channel_receiver.recv().await {
                yield PeerStreamMessage::ReqData(item);
            }
        })
            as Pin<Box<dyn Stream<Item = PeerStreamMessage> + Send>>;
        let delay_receiver = Box::pin(async_stream::stream! {
            while let Some(item) = delay_receiver.recv().await {
                yield PeerStreamMessage::ReqTimeout(item);
            }
        }) as Pin<Box<dyn Stream<Item = PeerStreamMessage> + Send>>;
        let pieces_bitfield_receiver = Box::pin(async_stream::stream! {
            while let Ok(_) = pieces_bitfield_receiver.changed().await {
                let item = pieces_bitfield_receiver.borrow_and_update().clone();
                yield PeerStreamMessage::BitfieldUpdate(item);
            }
        })
            as Pin<Box<dyn Stream<Item = PeerStreamMessage> + Send>>;
        let file_status_update = Box::pin(async_stream::stream! {
            while let Ok(_) = file_status_receiver.changed().await {
                let status = *file_status_receiver.borrow_and_update();
                yield PeerStreamMessage::FileStatusUpdate(status);
            }
        })
            as Pin<Box<dyn Stream<Item = PeerStreamMessage> + Send>>;
        let ctrl_c_recv = Box::pin(async_stream::stream! {
            while let Ok(_) = tokio::signal::ctrl_c().await {
                yield PeerStreamMessage::Terminate;
            }
        }) as Pin<Box<dyn Stream<Item = PeerStreamMessage> + Send>>;
        let mut stream_map = StreamMap::new();
        stream_map.insert("request_work", request_work_receiver);
        stream_map.insert("request_data", request_data_receiver);
        stream_map.insert("request_timeout", delay_receiver);
        stream_map.insert("bitfield_update", pieces_bitfield_receiver);
        stream_map.insert("file_status_update", file_status_update);
        stream_map.insert("ctrl_c", ctrl_c_recv);

        Ok(PeerTaskProcessor {
            our_bitfield,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            peer_bitfield,
            request_cache,
            cancels,
            stream_map,
            work_channel_sender,
            data_channel_sender,
            delay_map,
            delay_sender,
            request_work_sender,
            request_data_sender,
            data_sender,
            uploaded_bytes,
            continue_main_loop: true,
            file_complete: false,
        })
    }

    async fn main_loop(
        &mut self,
        peer: SocketAddr,
        client_id: String,
        info_hash: [u8; 20],
        tcp_stream: Option<TcpStream>,
    ) -> Result<(), BoxError> {
        debug!("Initiating TCP handshake");
        let handshaker = Handshaker {
            info_hash,
            client_id,
        };
        let (tcp_reader, tcp_writer) = timeout(
            Duration::from_secs(30),
            handshaker
                .handshake(tcp_stream.map_or(HandshakeType::Active(peer), HandshakeType::Passive)),
        )
        .await??
        .into_split();
        let mut tcp_writer = WriteWrapper::new(tcp_writer);

        if self.our_bitfield.at_least_one_piece_present() {
            let bitfield = self.our_bitfield.as_bytes();
            // bitfield can only come immediately after handshake
            if let Err(err) = tcp_writer
                .write_message(Message::BitField { bitfield })
                .await
            {
                error!("Couldn't send BitField: {}", err);
            } else {
                debug!("Bitfield message sent");
            }
        }

        if let Err(err) = tcp_writer.write_message(Message::Unchoke).await {
            error!("Couldn't send Unchoke: {}", err);
        } else {
            debug!("Unchoke message sent");
            self.am_choking = false;
        };

        let decoder = MessageDecoder;
        // double max block size (+ two u32's) so buffer should be able to always fit at least 1 message w/o realloc
        let framed_read = FramedRead::with_capacity(
            tcp_reader,
            decoder,
            (Message::BLOCK_SIZE_BYTES as usize + 8) * 2,
        );
        let tcp_stream = Box::pin(framed_read.map(PeerStreamMessage::TcpRead))
            as Pin<Box<dyn Stream<Item = PeerStreamMessage> + Send>>;
        self.stream_map.insert("tcp_read", tcp_stream);

        // TODO: keep alive
        // send if we don't send anything for 2mins to keep connection alive
        // (or maybe for us drop connection before then? who knows...)
        // conversely, if no messages for more than 2min from peer then drop connection

        // implement 2 timer tasks (which send to channel on completion)
        // one for us, one for them
        // when we send a message, reset our time, when we receive a message, reset their timer
        // can either drop connection when timer elapses, or send a KeepAlive (maybe have threshold of KeepAlives, when exceed no. then drop connection)
        // might need task to send Instant of completion to check race conditions, or dont bother

        while let Some((_, value)) = self.stream_map.next().await {
            match value {
                // listen for response to request for work piece
                PeerStreamMessage::ReqWork(option) => {
                    match option {
                        Some(work) => {
                            debug!("Received work: {:?}, rechecking work status", work);
                            if !self.request_cache.contains(&work) {
                                self.request_cache.push(work);
                            }
                            self.recheck_work_status(&mut tcp_writer).await;
                        }
                        None => {
                            // means no work available, disconnect if peer not interested
                            if !self.peer_interested {
                                debug!("No more work available, stopping loop");
                                self.continue_main_loop = false;
                            }
                        }
                    }
                }
                // listen for response to request for data
                PeerStreamMessage::ReqData(option) => match option {
                    Some(data) => {
                        self.upload_piece(data, &mut tcp_writer).await;
                    }
                    None => {
                        warn!("None from request for data from worker");
                    }
                },
                // listen for timeout on pending requests
                PeerStreamMessage::ReqTimeout(work_block) => {
                    self.process_request_timeout(work_block, &mut tcp_writer)
                        .await;
                }
                // listen for bitfield update
                PeerStreamMessage::BitfieldUpdate(bitfield) => {
                    self.process_bitfield_update(bitfield, &mut tcp_writer)
                        .await;
                }
                // when file completed, stop interest and dc if they aren't interested either
                PeerStreamMessage::FileStatusUpdate(status) => {
                    if status == FileStatus::Complete {
                        self.file_complete = true;
                        if self.am_interested {
                            if let Err(err) = tcp_writer.write_message(Message::NotInterested).await
                            {
                                error!("Couldn't send NotInterested message: {}", err);
                            } else {
                                debug!("NotInterested message sent");
                                self.am_interested = false;
                            }
                        }
                        if !self.peer_interested {
                            // they aren't interested in downloading from us, we aren't interested in downloading from them
                            debug!("Mutual interest ended, stopping loop");
                            self.continue_main_loop = false;
                        }
                        // TODO: send cancels for inflight requests?
                    }
                }
                // try getting bytes from peer
                PeerStreamMessage::TcpRead(result) => match result {
                    Ok(message) => self.process_message(message, &mut tcp_writer).await,
                    Err(err) => {
                        debug!("Couldn't read from TCP stream: {}", err);
                        self.continue_main_loop = false;
                    }
                },
                PeerStreamMessage::Terminate => {
                    debug!("SIGINT received, terminating");
                    self.continue_main_loop = false;
                }
            }
            if !self.continue_main_loop {
                break;
            }
        }
        Ok::<(), BoxError>(())
    }

    async fn upload_piece<T: AsyncWriteExt + Unpin>(
        &mut self,
        data: DataBlock,
        writer: &mut WriteWrapper<T>,
    ) {
        // only send Piece if wasn't cancelled in the meantime
        if self.cancels.remove(&data.as_work_block()) {
            debug!(
                "Request for Piece [{}] offset [{}] was cancelled",
                data.piece_index, data.block_begin_offset
            );
        } else {
            let data_len = data.block.len();
            if let Err(err) = writer
                .write_message(Message::Piece {
                    piece_index: data.piece_index as u32,
                    block_begin_offset: data.block_begin_offset as u32,
                    block: data.block,
                })
                .await
            {
                error!("Couldn't send Piece message: {}", err);
            } else {
                // update bytes uploaded stat
                debug!("Piece message sent, updating upload atomic");
                self.uploaded_bytes.fetch_add(data_len, Ordering::SeqCst);
            }
        }
    }

    async fn process_message<T: AsyncWriteExt + Unpin>(
        &mut self,
        message: Message,
        writer: &mut WriteWrapper<T>,
    ) {
        debug!(tcp_message = %message);
        match message {
            Message::KeepAlive => (),
            Message::Choke => {
                // cancel all current and future requests until unchoked
                self.peer_choking = true;
                self.delay_map
                    .iter()
                    .for_each(|(_, (sleep_task, _))| sleep_task.abort());
                self.delay_map.clear();
                self.request_cache.clear();
                // TODO: also inform worker to refresh timers on these work blocks? but timers not implemented yet
            }
            Message::Unchoke => {
                self.peer_choking = false;
                self.recheck_work_status(writer).await;
            }
            Message::Interested => {
                self.peer_interested = true;
            }
            Message::NotInterested => {
                self.peer_interested = false;
                // if we aren't interested, disconnect
                if !self.am_interested {
                    self.continue_main_loop = false;
                }
            }
            Message::Have { piece_index } => {
                if let Err(e) = self.peer_bitfield.set_has_piece(piece_index as usize) {
                    error!("Couldn't process Have: {}", e);
                } else {
                    self.recheck_work_status(writer).await;
                }
            }
            // ideally bitfield should come at most once, right after handshake (or not at all)
            Message::BitField { bitfield } => {
                match PiecesBitfield::from_bytes(&bitfield, self.our_bitfield.pieces_count()) {
                    Ok(peer_bitfield) => {
                        self.peer_bitfield.merge(&peer_bitfield);
                        self.recheck_work_status(writer).await;
                    }
                    Err(err) => {
                        error!("Couldn't parse peer Bitfield, dropping connection: {}", err);
                        self.continue_main_loop = false;
                    }
                }
            }
            Message::Request {
                piece_index,
                block_begin_offset,
                block_length,
            } => {
                if MIN_BLOCK_BYTES_LENGTH <= block_length && block_length <= MAX_BLOCK_BYTES_LENGTH
                {
                    // request data from worker
                    if let Err(err) = self.request_data_sender.send(RequestData {
                        piece_index: piece_index as usize,
                        block_begin_offset: block_begin_offset as usize,
                        block_length: block_length as usize,
                        sender: self.data_channel_sender.clone(),
                    }) {
                        error!("Couldn't send request for data to worker: {}", err);
                    } else {
                        debug!("Requesting data from worker");
                    }
                } else {
                    error!("Request block length out of range: {} bytes", block_length);
                }
            }
            Message::Piece {
                piece_index,
                block_begin_offset,
                block,
            } => {
                // send to worker task if file not complete, otherwise drop this data (and no need for further work)
                if !self.file_complete {
                    let block_length = block.len() as u32;
                    if let Err(err) = self.data_sender.send(DataBlock {
                        piece_index: piece_index as usize,
                        block_begin_offset: block_begin_offset as usize,
                        block,
                    }) {
                        error!("Couldn't send block data to worker: {}", err);
                    }
                    if let Err(err) = self.abort_timeout(WorkBlock {
                        piece_index,
                        block_begin_offset,
                        block_length,
                    }) {
                        warn!("Couldn't abort timeout: {}", err);
                    };
                    self.recheck_work_status(writer).await;
                }
            }
            Message::Cancel {
                piece_index,
                block_begin_offset,
                block_length,
            } => {
                self.cancels.insert(WorkBlock {
                    piece_index,
                    block_begin_offset,
                    block_length,
                });
            }
            Message::Port { listen_port } => (), // TODO: implement DHT
        }
    }

    async fn process_bitfield_update<T: AsyncWriteExt + Unpin>(
        &mut self,
        new_bitfield: PiecesBitfield,
        writer: &mut WriteWrapper<T>,
    ) {
        let delta_indices = self.our_bitfield.merge(&new_bitfield);
        // inform peer with Have messages
        let have_messages: Vec<Message> = delta_indices
            .iter()
            .map(|&index| Message::Have {
                piece_index: index as u32,
            })
            .collect();
        if let Err(err) = writer.write_multiple_messages(have_messages).await {
            error!("Couldn't send Have message(s): {}", err);
        } else {
            debug!("Have message(s) sent");
        }
    }

    async fn process_request_timeout<T: AsyncWriteExt + Unpin>(
        &mut self,
        work_block: WorkBlock,
        writer: &mut WriteWrapper<T>,
    ) {
        // in case was removed from map in meantime
        if let Entry::Occupied(mut entry) = self.delay_map.entry(work_block) {
            let retry_count = entry.get().1;
            // try resending, then after that just drop the work piece
            if retry_count < RETRY_COUNT {
                let task =
                    PeerTaskProcessor::send_request(work_block, writer, self.delay_sender.clone())
                        .await;
                entry.insert((task, retry_count + 1));
            } else {
                entry.remove_entry();
                self.recheck_work_status(writer).await;
            }
        }
    }

    async fn send_request<T: AsyncWriteExt + Unpin>(
        work_block: WorkBlock,
        writer: &mut WriteWrapper<T>,
        sender: mpsc::UnboundedSender<WorkBlock>,
    ) -> JoinHandle<()> {
        if let Err(err) = writer.write_message(work_block.to_request_message()).await {
            error!("Couldn't send Request message: {}", err);
        } else {
            debug!("Request message sent");
        }
        tokio::task::Builder::new()
            .name("Request timeout")
            .spawn(async move {
                sleep(RETRY_TIMEOUT_DURATION).await;
                // can panic here if peer task died, so just ignore the result of the send
                sender.send(work_block).ok();
            })
    }

    fn abort_timeout(&mut self, work_block: WorkBlock) -> CollectedResult<()> {
        if let Some((task, _)) = self.delay_map.remove(&work_block) {
            task.abort();
            Ok(())
        } else {
            Err(format!("Work block not in delay_map: {:?}", work_block).into())
        }
    }

    async fn recheck_work_status<T: AsyncWriteExt + Unpin>(
        &mut self,
        writer: &mut WriteWrapper<T>,
    ) {
        // don't bother doing anything if choked
        if self.peer_choking {
            return;
        }

        // check how many requests in flight, if maxed out then do nothing
        let mut free_space = REQUEST_BACKLOG_SIZE - self.delay_map.len();
        if free_space == 0 {
            return;
        }

        let mut current_work = self.request_cache.clone();
        current_work.extend(self.delay_map.keys());
        if self.request_cache.len() < free_space {
            // not enough requests in cache, request more from worker
            let d = free_space - self.request_cache.len();
            for _ in 0..d {
                if let Err(err) = self.request_work_sender.send(RequestWork::new(
                    &self.peer_bitfield,
                    &self.work_channel_sender,
                    &current_work,
                )) {
                    error!("Couldn't send request work message: {}", err);
                }
            }
        }

        if !self.request_cache.is_empty() {
            if !self.am_interested {
                if let Err(err) = writer.write_message(Message::Interested).await {
                    error!("Couldn't send Interested message: {}", err);
                    return;
                } else {
                    debug!("Interested message sent");
                    self.am_interested = true;
                }
            }

            while free_space > 0 && !self.request_cache.is_empty() {
                let work_block = self.request_cache.pop().unwrap();
                // only send work if not already in delay map (aka already pending from peer)
                if self.delay_map.contains_key(&work_block) {
                    continue;
                }
                let task =
                    PeerTaskProcessor::send_request(work_block, writer, self.delay_sender.clone())
                        .await;
                self.delay_map.insert(work_block, (task, 0));
                free_space -= 1;
            }
        }
    }
}

enum HandshakeType {
    Active(SocketAddr),
    Passive(TcpStream),
}

struct Handshaker {
    info_hash: [u8; 20],
    client_id: String,
}

impl Handshaker {
    fn get_our_handshake(&self) -> Vec<u8> {
        let mut our_handshake = Vec::with_capacity(68);
        our_handshake.push(19); // length of protocol identifier, 1 byte
        our_handshake.extend(b"BitTorrent protocol"); // protocol identifier, pstr, N bytes, N from above
        our_handshake.extend(vec![0; 8]); // 8 reserved bytes for extensions, 0 = extension not supported
        our_handshake.extend(self.info_hash); // 20 bytes
        our_handshake.extend(self.client_id.chars().map(|c| c as u8)); // 20 bytes
        our_handshake
    }

    fn check_peer_handshake(&self, peer_handshake: &[u8]) -> CollectedResult<()> {
        debug!(peer_handshake = ?peer_handshake);
        if peer_handshake[0] != 19 {
            Err("Wrong first byte in peer handshake".into())
        } else if peer_handshake[1..20] != *b"BitTorrent protocol" {
            Err("Wrong protocol name in peer handshake".into())
        } else if peer_handshake[28..48] != self.info_hash {
            Err("Wrong torrent hash in peer handshake".into())
        } else {
            let peer_id = &peer_handshake[48..68];
            let peer_extensions = &peer_handshake[20..28];
            debug!(peer_id = ?peer_id, peer_extensions = ?peer_extensions);
            Ok(())
        }
    }

    async fn handshake(&self, handshake_type: HandshakeType) -> Result<TcpStream, BoxError> {
        match handshake_type {
            HandshakeType::Active(peer) => self.active_handshake(peer).await,
            HandshakeType::Passive(stream) => self.passive_handshake(stream).await,
        }
    }

    // we are making a connection
    async fn active_handshake(&self, peer: SocketAddr) -> Result<TcpStream, BoxError> {
        debug!("Opening stream");
        let mut stream = TcpStream::connect(peer).await?;

        // send handshake
        let our_handshake = self.get_our_handshake();
        stream.write_all(&our_handshake).await?;

        let mut peer_handshake = vec![0; 68];
        debug!("Awaiting peer handshake response");
        stream.read_exact(&mut peer_handshake).await?;
        self.check_peer_handshake(&peer_handshake)?;

        Ok(stream)
    }

    // we are responding to a connection
    async fn passive_handshake(&self, mut stream: TcpStream) -> Result<TcpStream, BoxError> {
        debug!("Reading peer handshake");

        let mut peer_handshake = vec![0; 68];
        stream.read_exact(&mut peer_handshake).await?;
        self.check_peer_handshake(&peer_handshake)?;

        let our_handshake = self.get_our_handshake();
        stream.write_all(&our_handshake).await?;

        Ok(stream)
    }
}

struct WriteWrapper<T: AsyncWriteExt + Unpin> {
    writer: T,
}

impl<T: AsyncWriteExt + Unpin> WriteWrapper<T> {
    fn new(writer: T) -> WriteWrapper<T> {
        WriteWrapper { writer }
    }

    async fn write_message(&mut self, message: Message) -> Result<(), BoxError> {
        self.writer.write_all(&message.serialize()).await?;
        Ok(())
    }

    async fn write_multiple_messages(&mut self, messages: Vec<Message>) -> Result<(), BoxError> {
        let bytes: Vec<u8> = messages.iter().flat_map(|msg| msg.serialize()).collect();
        self.writer.write_all(&bytes).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum Message {
    // indices are 0-based
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        piece_index: u32,
    },
    BitField {
        bitfield: Vec<u8>,
    },
    Request {
        piece_index: u32,
        block_begin_offset: u32,
        block_length: u32, // range: [16KB, 128KB]
    },
    Piece {
        piece_index: u32,
        block_begin_offset: u32,
        block: Vec<u8>,
    },
    // TODO: implement endgame mode, as described below
    // When a download is almost complete, there's a tendency for the last few blocks to trickle in slowly.
    // To speed this up, the client sends requests for all of its missing blocks to all of its peers.
    // To keep this from becoming horribly inefficient, the client also sends a cancel to everyone else every time a block arrives.
    Cancel {
        piece_index: u32,
        block_begin_offset: u32,
        block_length: u32,
    },
    Port {
        listen_port: u16,
    },
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::KeepAlive => write!(f, "KeepAlive"),
            Message::Choke => write!(f, "Choke"),
            Message::Unchoke => write!(f, "Unchoke"),
            Message::Interested => write!(f, "Interested"),
            Message::NotInterested => write!(f, "NotInterested"),
            Message::Have { piece_index } => write!(f, "Have {{ piece_index: {} }}", piece_index),
            Message::BitField { bitfield } => {
                write!(f, "BitField {{ bitfield.len(): {} }}", bitfield.len())
            }
            Message::Request {
                piece_index,
                block_begin_offset,
                block_length,
            } => write!(
                f,
                "Request {{ piece_index: {}, block_begin_offset: {}, block_length: {} }}",
                piece_index, block_begin_offset, block_length
            ),
            Message::Piece {
                piece_index,
                block_begin_offset,
                block,
            } => write!(
                f,
                "Piece {{ piece_index: {}, block_begin_offset: {}, block.len(): {} }}",
                piece_index,
                block_begin_offset,
                block.len()
            ),
            Message::Cancel {
                piece_index,
                block_begin_offset,
                block_length,
            } => write!(
                f,
                "Cancel {{ piece_index: {}, block_begin_offset: {}, block_length: {} }}",
                piece_index, block_begin_offset, block_length
            ),
            Message::Port { listen_port } => write!(f, "Port {{ listen_port: {} }}", listen_port),
        }
    }
}

impl Message {
    pub const BLOCK_SIZE_BYTES: u32 = 16 * 1024; // TODO: can't bump this up? any implicit dependencies on this in rest of code?

    fn serialize(&self) -> Vec<u8> {
        // <length><id><payload>
        // where length is 4 byte big-endian and id is 1 byte
        // some messages have no payload, keep alive has no id
        match self {
            Message::KeepAlive => vec![0; 4],
            Message::Choke | Message::Unchoke | Message::Interested | Message::NotInterested => {
                vec![0, 0, 0, 1, self.id()]
            }
            Message::Have { piece_index } => {
                let mut bytes = vec![0, 0, 0, 5, self.id(), 0, 0, 0, 0];
                BigEndian::write_u32(&mut bytes[5..9], *piece_index);
                bytes
            }
            Message::Request {
                piece_index,
                block_begin_offset,
                block_length,
            }
            | Message::Cancel {
                piece_index,
                block_begin_offset,
                block_length,
            } => {
                let mut bytes = vec![0, 0, 0, 13, self.id(), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
                BigEndian::write_u32_into(
                    &[*piece_index, *block_begin_offset, *block_length],
                    &mut bytes[5..17],
                );
                bytes
            }
            Message::BitField { bitfield } => {
                let mut bytes = vec![0, 0, 0, 0, self.id()];
                BigEndian::write_u32_into(&[1 + bitfield.len() as u32], &mut bytes[0..4]);
                bytes.extend_from_slice(bitfield);
                bytes
            }
            Message::Piece {
                piece_index,
                block_begin_offset,
                block,
            } => {
                let mut bytes = vec![0, 0, 0, 0, self.id(), 0, 0, 0, 0, 0, 0, 0, 0];
                BigEndian::write_u32_into(&[9 + block.len() as u32], &mut bytes[0..4]);
                BigEndian::write_u32_into(&[*piece_index, *block_begin_offset], &mut bytes[5..13]);
                bytes.extend_from_slice(block);
                bytes
            }
            Message::Port { listen_port } => {
                let mut bytes = vec![0, 0, 0, 3, self.id(), 0, 0];
                BigEndian::write_u16(&mut bytes[5..7], *listen_port);
                bytes
            }
        }
    }

    // payload is excluding length and id bytes!
    fn from_id(id: u8, payload: &[u8]) -> CollectedResult<Message> {
        match id {
            0 => Ok(Message::Choke),
            1 => Ok(Message::Unchoke),
            2 => Ok(Message::Interested),
            3 => Ok(Message::NotInterested),
            4 => Ok(Message::Have {
                piece_index: BigEndian::read_u32(&payload[0..4]),
            }),
            5 => Ok(Message::BitField {
                bitfield: payload.to_vec(),
            }),
            6 => Ok(Message::Request {
                piece_index: BigEndian::read_u32(&payload[0..4]),
                block_begin_offset: BigEndian::read_u32(&payload[4..8]),
                block_length: BigEndian::read_u32(&payload[8..12]),
            }),
            7 => Ok(Message::Piece {
                piece_index: BigEndian::read_u32(&payload[0..4]),
                block_begin_offset: BigEndian::read_u32(&payload[4..8]),
                block: payload[8..].to_vec(),
            }),
            8 => Ok(Message::Cancel {
                piece_index: BigEndian::read_u32(&payload[0..4]),
                block_begin_offset: BigEndian::read_u32(&payload[4..8]),
                block_length: BigEndian::read_u32(&payload[8..12]),
            }),
            9 => Ok(Message::Port {
                listen_port: BigEndian::read_u16(&payload[0..2]),
            }),
            _ => Err(CollectedError::MalformedMessageBytes(format!(
                "Unrecognized message id: {}",
                id
            ))),
        }
    }

    fn id(&self) -> u8 {
        match &*self {
            Message::KeepAlive => unreachable!(),
            Message::Choke => 0,
            Message::Unchoke => 1,
            Message::Interested => 2,
            Message::NotInterested => 3,
            Message::Have { piece_index: _ } => 4,
            Message::BitField { bitfield: _ } => 5,
            Message::Request {
                piece_index: _,
                block_begin_offset: _,
                block_length: _,
            } => 6,
            Message::Piece {
                piece_index: _,
                block_begin_offset: _,
                block: _,
            } => 7,
            Message::Cancel {
                piece_index: _,
                block_begin_offset: _,
                block_length: _,
            } => 8,
            Message::Port { listen_port: _ } => 9,
        }
    }
}

struct MessageDecoder;

impl Decoder for MessageDecoder {
    type Item = Message;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        // get length
        let mut length = [0; 4];
        length.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length) as usize;

        // error if message is sized > 1024 kb
        if length > 1024 * 1024 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Length too large: {}", length),
            ));
        }

        if length == 0 {
            src.advance(4);
            return Ok(Some(Message::KeepAlive));
        }

        if src.len() < 4 + length {
            // ensure have enough space if not enough
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        src.advance(4);
        let id = src.get_u8();
        let payload = &src[..length - 1];
        let message = Message::from_id(id, payload);
        src.advance(length - 1);
        match message {
            Ok(msg) => Ok(Some(msg)),
            Err(err) => Err(Error::new(ErrorKind::InvalidData, err)),
        }
    }
}

#[cfg(test)]
mod test {
    use bytes::BufMut;

    use super::*;

    #[test]
    fn work_block_hash() {
        let work_block = WorkBlock {
            piece_index: 1,
            block_begin_offset: 2,
            block_length: 3,
        };
        let mut map = HashMap::new();
        map.insert(work_block, "a");
        println!("{:?}", map);
        let re = map.get(&work_block);
        println!("{:?}", re);
    }

    #[tokio::test]
    async fn test_stream_receiver() {
        let (work_channel_sender, mut work_channel_receiver) = mpsc::unbounded_channel::<u32>();

        let mut stream_map = StreamMap::new();

        let request_work_receiver = Box::pin(async_stream::stream! {
            while let Some(item) = work_channel_receiver.recv().await {
                yield item;
            }
        }) as Pin<Box<dyn Stream<Item = u32> + Send>>;
        stream_map.insert("test", request_work_receiver);

        let work_channel_sender1 = work_channel_sender.clone();
        tokio::spawn(async move {
            let r = work_channel_sender1.send(5);
            println!("Task 1: {:?}", r);
        });

        let work_channel_sender2 = work_channel_sender.clone();
        tokio::spawn(async move {
            let r = work_channel_sender2.send(10);
            println!("Task 2: {:?}", r);
        });

        let r = work_channel_sender.send(15);
        println!("Task base: {:?}", r);

        while let Some(msg) = stream_map.next().await {
            println!("Received: {:?}", msg);
        }
    }

    #[test]
    fn test_message_decoder() {
        let mut decoder = MessageDecoder;
        let mut buf = bytes::BytesMut::with_capacity(128);

        let msg = Message::KeepAlive;
        buf.put_slice(&msg.serialize());

        let msg = Message::Choke;
        buf.put_slice(&msg.serialize());

        let msg = Message::Have { piece_index: 4 };
        buf.put_slice(&msg.serialize());

        let msg = Message::Piece {
            piece_index: 4,
            block_begin_offset: 2,
            block: vec![1, 2, 3, 4],
        };
        buf.put_slice(&msg.serialize());

        // malformed message
        buf.put_u32(5);
        buf.put_u8(10);
        buf.put_u32(100);

        // assertions
        assert!(matches!(
            decoder.decode(&mut buf),
            Ok(Some(Message::KeepAlive))
        ));

        assert!(matches!(decoder.decode(&mut buf), Ok(Some(Message::Choke))));

        assert!(matches!(
            decoder.decode(&mut buf),
            Ok(Some(Message::Have { piece_index: 4 }))
        ));

        let _data = vec![1, 2, 3, 4];
        assert!(matches!(
            decoder.decode(&mut buf),
            Ok(Some(Message::Piece {
                piece_index: 4,
                block_begin_offset: 2,
                block: _data,
            }))
        ));

        assert!(decoder.decode(&mut buf).is_err());
    }
}
