use crate::{
    peer_connection::{get_listener_task, get_peer_task},
    tracker::{TrackerRequestEvent, TrackerTaskState},
    worker::{get_worker_task, DataBlock, FileData, PiecesBitfield, RequestData, RequestWork},
};
use clap::Parser;
use error::BoxError;
use futures::Stream;
use rand::{
    distributions::Alphanumeric,
    prelude::{IteratorRandom, ThreadRng},
    Rng,
};
use regex::Regex;
use std::{
    collections::{hash_map::Entry, HashMap},
    fs,
    net::SocketAddr,
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_stream::{StreamExt, StreamMap};
use torrent_info::TorrentInfo;
use tracing::{debug, error, info, metadata::LevelFilter};
use tracing_subscriber::{
    fmt::{self, time::LocalTime},
    prelude::__tracing_subscriber_SubscriberExt,
    util::SubscriberInitExt,
    Layer,
};

mod bencode;
mod error;
mod magnet;
mod peer_connection;
mod torrent_info;
mod tracker;
mod worker;

#[macro_use]
extern crate derive_builder;

const MAX_ACTIVE_CONCURRENT_PEERS: usize = 15; // connections we make
const MAX_PASSIVE_CONCURRENT_PEERS: usize = 15; // connections via listener
const LOG_FILE_NAME: &str = "client.log";

#[derive(Parser, Debug)]
#[clap(about, version, author)]
struct Args {
    #[clap(short = 'f', long)]
    torrent_file: String,

    #[clap(short = 'p', long)]
    client_port: u16,

    // For uses such as port forwarding
    #[clap(short = 'x', long)]
    external_port: Option<u16>,

    // If IP of client is different from requesting IP
    #[clap(short = 'a', long)]
    external_ip: Option<String>,

    // If want to specify 20 byte ID of the client
    #[clap(short='c',
        long,
        validator_regex(
            Regex::new(r"^[A-Za-z0-9_-]{20}$").unwrap(),
            "Client ID must be 20 character Alphanumeric"
        )
    )]
    client_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileStatus {
    Complete,
    Incomplete,
}

enum PeerCheckerStreamMessage {
    PeerVecUpdate(Vec<SocketAddr>),
    PeerTaskCompletion(SocketAddr),
    FileCompleteUpdate(FileStatus),
    Terminate,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    {
        // truncating log file each time
        std::fs::File::create(LOG_FILE_NAME).unwrap();
    }
    // let console_layer = console_subscriber::spawn();
    let file_appender = tracing_appender::rolling::never(".", LOG_FILE_NAME);
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::registry()
        // tokio console
        // .with(console_layer)
        // file
        .with(
            fmt::Layer::new()
                .with_writer(non_blocking)
                .with_timer(LocalTime::rfc_3339())
                .with_ansi(false)
                .with_filter(LevelFilter::DEBUG),
        )
        // stdout
        .with(
            fmt::Layer::new()
                .with_writer(std::io::stdout)
                .with_timer(LocalTime::rfc_3339())
                .with_ansi(true)
                .with_filter(LevelFilter::INFO),
        )
        .init();

    let args = Args::parse();
    let file = fs::read(args.torrent_file)?;
    let bencode = bencode::parse(file.iter())?;
    let torrent_info = torrent_info::TorrentInfo::new(bencode)?;

    if torrent_info.pieces.len() % 20 != 0 {
        return Err(format!(
            "Error parsing torrent info file: pieces length not divisible by 20: {}",
            torrent_info.pieces.len()
        )
        .into());
    }

    let pieces_count = torrent_info.pieces.len() / 20;

    let client_id: String = match args.client_id {
        Some(id) => id,
        None => rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(20)
            .map(char::from)
            .collect(),
    };

    info!("Client ID set to: [{}]", client_id);

    let local_port = args.client_port;
    let external_port = match args.external_port {
        Some(p) => p,
        None => local_port,
    };

    let file_data = FileData::new(
        &torrent_info.name,
        pieces_count,
        torrent_info.length as usize,
        torrent_info.piece_length as usize,
        &torrent_info.pieces,
    )
    .await?;

    //////////////////
    // Tracker task //
    //////////////////
    let (peer_vec_sender, mut peer_vec_receiver) = mpsc::unbounded_channel::<Vec<SocketAddr>>();
    let downloaded_bytes = Arc::new(AtomicUsize::new(0));
    let uploaded_bytes = Arc::new(AtomicUsize::new(0));

    let mut tracker_task_state = TrackerTaskState::new(
        &torrent_info.announce,
        &torrent_info.info_hash,
        &client_id,
        external_port,
        &downloaded_bytes,
        &uploaded_bytes,
        torrent_info.length as usize,
        args.external_ip,
        peer_vec_sender,
    );
    let peers = tracker_task_state
        .make_request(TrackerRequestEvent::Started)
        .await?;

    let (file_status_sender, file_status_receiver) = watch::channel(FileStatus::Incomplete);
    let tracker_task = tokio::task::Builder::new()
        .name("Tracker")
        .spawn(tracker_task_state.get_tracker_task(file_status_receiver.clone()));

    /////////////////
    // Worker task //
    /////////////////
    let (request_work_sender, request_work_receiver) = mpsc::unbounded_channel::<RequestWork>();
    let (request_data_sender, request_data_receiver) = mpsc::unbounded_channel::<RequestData>();
    let (data_sender, data_receiver) = mpsc::unbounded_channel::<DataBlock>();
    let (pieces_bitfield_sender, pieces_bitfield_receiver) =
        watch::channel::<PiecesBitfield>(file_data.get_bitfield());

    let worker_task = tokio::task::Builder::new()
        .name("Worker")
        .spawn(get_worker_task(
            file_data,
            request_data_receiver,
            request_work_receiver,
            data_receiver,
            pieces_bitfield_sender,
            file_status_sender,
            downloaded_bytes,
        ));

    ///////////////////
    // Listener task //
    ///////////////////
    let listener_task = tokio::task::Builder::new()
        .name("Listener")
        .spawn(get_listener_task(
            local_port,
            MAX_PASSIVE_CONCURRENT_PEERS,
            client_id.clone(),
            torrent_info.info_hash,
            pieces_bitfield_receiver.clone(),
            file_status_receiver.clone(),
            request_work_sender.clone(),
            request_data_sender.clone(),
            data_sender.clone(),
            uploaded_bytes.clone(),
        ));

    ////////////////
    // Peer tasks //
    ////////////////
    // address -> (total number of connections so far, task)
    let mut peers_map: HashMap<SocketAddr, (u32, Option<JoinHandle<_>>)> =
        peers.iter().map(|&p| (p, (0, None))).collect();

    let (peer_task_completion_sender, mut peer_task_completion_receiver) =
        mpsc::unbounded_channel::<SocketAddr>();

    let mut rng = rand::thread_rng();
    let initial_peers: Vec<SocketAddr> = peers_map
        .iter()
        .map(|(&addr, _)| addr)
        .choose_multiple(&mut rng, MAX_ACTIVE_CONCURRENT_PEERS);

    debug!(initial_peers = ?initial_peers);

    initial_peers.iter().for_each(|&peer| {
        let task = tokio::task::Builder::new()
            .name(&format!("Active {}", peer))
            .spawn(get_peer_task(
                peer,
                client_id.clone(),
                torrent_info.info_hash,
                pieces_bitfield_receiver.clone(),
                file_status_receiver.clone(),
                request_work_sender.clone(),
                request_data_sender.clone(),
                data_sender.clone(),
                peer_task_completion_sender.clone(),
                uploaded_bytes.clone(),
                None,
            ));
        *peers_map.get_mut(&peer).unwrap() = (1, Some(task));
    });

    let mut no_active_connections = initial_peers.len();

    // prepare streams
    let peer_vec_receiver = Box::pin(async_stream::stream! {
        while let Some(item) = peer_vec_receiver.recv().await {
            yield PeerCheckerStreamMessage::PeerVecUpdate(item);
        }
    }) as Pin<Box<dyn Stream<Item = PeerCheckerStreamMessage> + Send>>;
    let peer_task_completion_receiver = Box::pin(async_stream::stream! {
        while let Some(item) = peer_task_completion_receiver.recv().await {
            yield PeerCheckerStreamMessage::PeerTaskCompletion(item);
        }
    })
        as Pin<Box<dyn Stream<Item = PeerCheckerStreamMessage> + Send>>;
    let mut stream_file_status_receiver = file_status_receiver.clone();
    let file_status_update = Box::pin(async_stream::stream! {
        while let Ok(_) = stream_file_status_receiver.changed().await {
            let status = *stream_file_status_receiver.borrow_and_update();
            yield PeerCheckerStreamMessage::FileCompleteUpdate(status);
        }
    }) as Pin<Box<dyn Stream<Item = PeerCheckerStreamMessage> + Send>>;
    let ctrl_c_recv = Box::pin(async_stream::stream! {
        while let Ok(_) = tokio::signal::ctrl_c().await {
            yield PeerCheckerStreamMessage::Terminate;
        }
    }) as Pin<Box<dyn Stream<Item = PeerCheckerStreamMessage> + Send>>;

    let mut stream_map = StreamMap::new();
    stream_map.insert("peer_vec_receiver", peer_vec_receiver);
    stream_map.insert(
        "peer_task_completion_receiver",
        peer_task_completion_receiver,
    );
    stream_map.insert("file_status_update", file_status_update);
    stream_map.insert("ctrl_c", ctrl_c_recv);

    while let Some((key, value)) = stream_map.next().await {
        debug!(stream_message_type = key);
        match value {
            PeerCheckerStreamMessage::PeerVecUpdate(peers_vec) => {
                // merge new peers into the hashmap
                peers_vec.iter().for_each(|&addr| {
                    peers_map.entry(addr).or_insert((0, None));
                });

                spawn_peer_task(
                    &mut no_active_connections,
                    &mut peers_map,
                    &mut rng,
                    &client_id,
                    &torrent_info,
                    &pieces_bitfield_receiver,
                    &file_status_receiver,
                    &request_work_sender,
                    &request_data_sender,
                    &data_sender,
                    &peer_task_completion_sender,
                    &uploaded_bytes,
                );
            }
            PeerCheckerStreamMessage::PeerTaskCompletion(peer) => {
                // housekeeping
                no_active_connections -= 1;
                if let Entry::Occupied(mut entry) = peers_map.entry(peer) {
                    let count = entry.get().0;
                    entry.insert((count, None));

                    spawn_peer_task(
                        &mut no_active_connections,
                        &mut peers_map,
                        &mut rng,
                        &client_id,
                        &torrent_info,
                        &pieces_bitfield_receiver,
                        &file_status_receiver,
                        &request_work_sender,
                        &request_data_sender,
                        &data_sender,
                        &peer_task_completion_sender,
                        &uploaded_bytes,
                    );
                }
            }
            PeerCheckerStreamMessage::FileCompleteUpdate(status) => {
                if status == FileStatus::Complete {
                    info!("File complete, no longer actively making connections");
                    break;
                }
            }
            PeerCheckerStreamMessage::Terminate => {
                info!("SIGINT received, terminating");
                break;
            }
        }
    }

    futures::future::join_all(
        peers_map
            .into_iter()
            .filter_map(|(_, (_, opt_fut))| opt_fut),
    )
    .await;

    let result = tokio::join!(worker_task, tracker_task, listener_task);
    info!("Core task exit statuses: {:?}", result);
    info!("Goodbye");
    Ok(())
}

fn spawn_peer_task(
    no_active_connections: &mut usize,
    peers_map: &mut HashMap<SocketAddr, (u32, Option<JoinHandle<Result<(), BoxError>>>)>,
    rng: &mut ThreadRng,
    client_id: &str,
    torrent_info: &TorrentInfo,
    pieces_bitfield_receiver: &watch::Receiver<PiecesBitfield>,
    file_status_receiver: &watch::Receiver<FileStatus>,
    request_work_sender: &mpsc::UnboundedSender<RequestWork>,
    request_data_sender: &mpsc::UnboundedSender<RequestData>,
    data_sender: &mpsc::UnboundedSender<DataBlock>,
    peer_task_completion_sender: &mpsc::UnboundedSender<SocketAddr>,
    uploaded_bytes: &Arc<AtomicUsize>,
) {
    // check current tasks, spin up new ones if under limit
    let free_slots = MAX_ACTIVE_CONCURRENT_PEERS - *no_active_connections;
    if free_slots > 0 {
        debug!(
            "Free peer task slots available, spinning up {} tasks",
            free_slots
        );
        // spin up new tasks
        for _ in 0..free_slots {
            if let Some(min_count) = peers_map
                .values()
                .filter_map(|(count, opt)| if opt.is_none() { Some(count) } else { None })
                .min_by(Ord::cmp)
            {
                if let Some(peer) = peers_map
                    .iter()
                    .filter_map(|(&addr, (count, opt))| {
                        if opt.is_none() && *count == *min_count {
                            Some(addr)
                        } else {
                            None
                        }
                    })
                    .choose(rng)
                {
                    let task = tokio::task::Builder::new()
                        .name(&format!("Active {}", peer))
                        .spawn(get_peer_task(
                            peer,
                            client_id.to_string(),
                            torrent_info.info_hash,
                            pieces_bitfield_receiver.clone(),
                            file_status_receiver.clone(),
                            request_work_sender.clone(),
                            request_data_sender.clone(),
                            data_sender.clone(),
                            peer_task_completion_sender.clone(),
                            uploaded_bytes.clone(),
                            None,
                        ));
                    *peers_map.get_mut(&peer).unwrap() = (min_count + 1, Some(task));
                    *no_active_connections += 1;
                } else {
                    error!("Couldn't find peer with min count when spawning peer tasks");
                }
            } else {
                error!("Couldn't find min count when spawning peer tasks");
            }
        }
    }
}
