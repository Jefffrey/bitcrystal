use crate::{
    bencode::{self, BencodeValue},
    error::{BoxError, CollectedError, CollectedResult},
    FileStatus,
};
use byteorder::{BigEndian, ByteOrder};
use reqwest::Client;
use std::{
    collections::HashMap,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    time::{interval_at, Instant, MissedTickBehavior},
};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::{debug, error, error_span, info, warn, Instrument};

const MAX_RETRY_COUNT: u32 = 5;
const RETRY_FLAT_WAIT_SECONDS: u64 = 5;

enum TrackerStreamMessage {
    IntervalUpdate,
    FileStatusUpdate(FileStatus),
    Terminate,
}

#[derive(Debug)]
pub struct TrackerTaskState {
    announce_url: String,
    info_hash: String,
    client_id: String,
    client_port: u16,

    peer_vec_sender: mpsc::UnboundedSender<Vec<SocketAddr>>,
    downloaded_bytes: Arc<AtomicUsize>,
    uploaded_bytes: Arc<AtomicUsize>,
    file_size: usize,

    interval_seconds: usize,
    tracker_id: Option<String>,
    client_ip: Option<String>, // dotted quad format

    client: Client,
}

impl TrackerTaskState {
    // TODO: do this as a builder instead?
    pub fn new(
        announce_url: &str,
        info_hash: &[u8; 20],
        client_id: &str,
        client_port: u16,
        downloaded_bytes: &Arc<AtomicUsize>,
        uploaded_bytes: &Arc<AtomicUsize>,
        file_size: usize,
        client_ip: Option<String>,
        peer_vec_sender: mpsc::UnboundedSender<Vec<SocketAddr>>,
    ) -> TrackerTaskState {
        TrackerTaskState {
            announce_url: announce_url.to_string(),
            info_hash: urlencoding::encode_binary(info_hash).into_owned(),
            client_id: client_id.to_string(),
            client_port,
            peer_vec_sender,
            downloaded_bytes: downloaded_bytes.clone(),
            uploaded_bytes: uploaded_bytes.clone(),
            file_size,
            interval_seconds: 0,
            tracker_id: None,
            client_ip: client_ip.map(|s| urlencoding::encode(&s).into_owned()),
            client: Client::builder()
                .pool_max_idle_per_host(0)
                .pool_idle_timeout(Some(Duration::from_secs(5)))
                .build()
                .unwrap(),
        }
    }

    pub async fn make_request(
        &mut self,
        event: TrackerRequestEvent,
    ) -> Result<Vec<SocketAddr>, BoxError> {
        let downloaded_bytes = self.downloaded_bytes.load(Ordering::SeqCst);

        let request = TrackerRequestBuilder::default()
            .info_hash(self.info_hash.clone())
            .client_id(self.client_id.clone())
            .client_port(self.client_port)
            .uploaded_bytes(self.uploaded_bytes.load(Ordering::SeqCst))
            .downloaded_bytes(downloaded_bytes)
            // in case downloaded_bytes too much, though that should be fixed now anyway
            .bytes_left(self.file_size.saturating_sub(downloaded_bytes))
            .event(event)
            .client_ip(self.client_ip.clone())
            .tracker_id(self.tracker_id.clone())
            .build()?;

        debug!(tracker_request = ?request);

        // TODO: use reqwest lib to clean this up
        let query_params = request
            .take_as_map()
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .reduce(|a, b| format!("{}&{}", a, b))
            .unwrap();
        let url = format!("{}?{}", self.announce_url, query_params);

        debug!(get_url = %url);

        let resp = {
            let mut r = self.client.get(&url).send().await;
            let mut count = 0;
            while r.is_err() && count < MAX_RETRY_COUNT {
                warn!(
                    "Response from tracker: {:?}, retrying after {} seconds (retry count {})",
                    r, RETRY_FLAT_WAIT_SECONDS, count
                );
                count += 1;
                tokio::time::sleep(Duration::from_secs(RETRY_FLAT_WAIT_SECONDS)).await;
                r = self.client.get(&url).send().await;
            }
            r?
        };

        debug!(tracker_raw_response = ?resp);

        let bytes = resp.bytes().await?;
        let response = bencode::parse(bytes.iter())?;
        let tracker_response = TrackerResponse::parse_from_bencode(&response)?;

        debug!(tracker_response = ?tracker_response);
        info!(seeders = tracker_response.seeders);
        info!(leechers = tracker_response.leechers);
        info!(interval_seconds = tracker_response.interval_seconds);

        if let Some(msg) = tracker_response.warning_message {
            warn!(message = %msg);
        }

        // updating data from the response
        self.interval_seconds = tracker_response.interval_seconds;
        if let Some(id) = tracker_response.tracker_id {
            self.tracker_id = Some(id)
        }

        Ok(tracker_response.peers)
    }

    pub fn get_tracker_task(
        mut self,
        mut file_status_receiver: watch::Receiver<FileStatus>,
    ) -> impl Future<Output = ()> {
        // convert channels to streams
        let duration = Duration::from_secs(self.interval_seconds as u64);
        let interval_update = Box::pin(async_stream::stream! {
            // TODO: be able to change duration based on further requests to tracker, in case value changes (unlikely though?)
            let mut interval = interval_at(Instant::now() + duration, duration);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                yield TrackerStreamMessage::IntervalUpdate;
            }
        })
            as Pin<Box<dyn Stream<Item = TrackerStreamMessage> + Send>>;
        let file_status_update = Box::pin(async_stream::stream! {
            while let Ok(_) = file_status_receiver.changed().await {
                let status = *file_status_receiver.borrow_and_update();
                yield TrackerStreamMessage::FileStatusUpdate(status);
            }
        })
            as Pin<Box<dyn Stream<Item = TrackerStreamMessage> + Send>>;
        let ctrl_c_recv = Box::pin(async_stream::stream! {
            while let Ok(_) = tokio::signal::ctrl_c().await {
                yield TrackerStreamMessage::Terminate;
            }
        }) as Pin<Box<dyn Stream<Item = TrackerStreamMessage> + Send>>;

        let mut stream_map = StreamMap::new();
        stream_map.insert("interval_update", interval_update);
        stream_map.insert("file_status_update", file_status_update);
        stream_map.insert("ctrl_c", ctrl_c_recv);

        async move {
            while let Some((key, value)) = stream_map.next().await {
                debug!(stream_message_type = key);
                match value {
                    // regular calls to tracker to update data
                    TrackerStreamMessage::IntervalUpdate => {
                        match self.make_request(TrackerRequestEvent::Regular).await {
                            Ok(peers) => {
                                if let Err(err) = self.peer_vec_sender.send(peers) {
                                    error!("Couldn't send peer vec: {}", err);
                                } else {
                                    info!("Successful regular GET Request and update of peer vec");
                                }
                            }
                            Err(e) => error!("Couldn't make Regular GET to tracker: {}", e),
                        }
                    }
                    // when file completed, inform irregularly
                    TrackerStreamMessage::FileStatusUpdate(status) => {
                        if status == FileStatus::Complete {
                            if let Err(e) = self.make_request(TrackerRequestEvent::Completed).await
                            {
                                error!("Couldn't make Complete GET to tracker: {}", e);
                            } else {
                                info!("Successful Complete GET request");
                            }
                        } else {
                            warn!("File status update wasn't Complete");
                        }
                    }
                    // terminate gracefully
                    TrackerStreamMessage::Terminate => {
                        info!("SIGINT received, terminating");
                        if let Err(e) = self.make_request(TrackerRequestEvent::Stopped).await {
                            error!("Couldn't make Stopped GET to tracker: {}", e);
                        } else {
                            info!("Successful Stopped GET request");
                        }
                        break;
                    }
                }
            }
        }
        .instrument(error_span!("Tracker"))
    }
}

#[derive(Copy, Clone, Debug)]
pub enum TrackerRequestEvent {
    Started,
    Completed,
    Stopped,
    Regular,
}

#[derive(Builder, Debug)]
pub struct TrackerRequest {
    info_hash: String,
    client_id: String,
    client_port: u16,
    uploaded_bytes: usize,
    downloaded_bytes: usize,
    bytes_left: usize,
    event: TrackerRequestEvent,
    #[builder(default)]
    client_ip: Option<String>,
    #[builder(default)]
    no_of_peers_requested: Option<u32>,
    #[builder(default)]
    tracker_id: Option<String>,
}

impl TrackerRequest {
    fn take_as_map(self) -> HashMap<&'static str, String> {
        let mut map = HashMap::from([
            ("info_hash", self.info_hash),
            ("peer_id", self.client_id),
            ("port", self.client_port.to_string()),
            ("uploaded", self.uploaded_bytes.to_string()),
            ("downloaded", self.downloaded_bytes.to_string()),
            ("compact", "1".to_string()), // compact=1 (packed peer list, 6 bytes per peer)
            ("left", self.bytes_left.to_string()),
            ("event", "started".to_string()),
        ]);

        match self.event {
            TrackerRequestEvent::Started => map.insert("event", "started".to_string()),
            TrackerRequestEvent::Completed => map.insert("event", "completed".to_string()),
            TrackerRequestEvent::Stopped => map.insert("event", "stopped".to_string()),
            TrackerRequestEvent::Regular => None, // if regular call, don't need this key
        };

        if let Some(ip) = self.client_ip {
            map.insert("ip", ip);
        }

        if let Some(num) = self.no_of_peers_requested {
            map.insert("numwant", num.to_string());
        }

        if let Some(id) = self.tracker_id {
            map.insert("trackerid", id);
        }

        map
    }
}

#[derive(Debug)]
pub struct TrackerResponse {
    interval_seconds: usize,
    tracker_id: Option<String>,
    seeders: usize,
    leechers: usize,
    peers: Vec<SocketAddr>,
    warning_message: Option<String>,
}

impl TrackerResponse {
    fn parse_from_bencode(bencode: &BencodeValue) -> CollectedResult<TrackerResponse> {
        let response_root = bencode.get_dictionary()?;

        if let Some(msg) = response_root.get("failure reason") {
            return Err(msg.get_string()?.clone().into());
        } else {
            let interval_seconds = *response_root
                .get("interval")
                .ok_or_else(|| CollectedError::MissingKeyError("interval".to_string()))?
                .get_integer()? as usize;

            let tracker_id = response_root
                .get("tracker id")
                .and_then(|opt| opt.get_string().ok())
                .cloned();

            let seeders = *response_root
                .get("complete")
                .ok_or_else(|| CollectedError::MissingKeyError("complete".to_string()))?
                .get_integer()? as usize;

            let leechers = *response_root
                .get("incomplete")
                .ok_or_else(|| CollectedError::MissingKeyError("incomplete".to_string()))?
                .get_integer()? as usize;

            let peers = {
                // peers is ipv4, peers6 is ipv6
                if let Some(peers_value) = response_root.get("peers") {
                    peers_value
                        .get_string()?
                        .chars() // chars NOT bytes! since would be bytes of utf-8 string, not the byte string
                        .map(|c| c as u8)
                        .collect::<Vec<u8>>()
                        .chunks_exact(6)
                        .map(|bytes| {
                            SocketAddr::new(
                                IpAddr::V4(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])),
                                BigEndian::read_u16(&bytes[4..6]),
                            )
                        })
                        .collect::<Vec<SocketAddr>>()
                } else {
                    // sometimes peer is missing (e.g. Stopped request)
                    vec![]
                }
            };

            let warning_message = response_root
                .get("warning message")
                .and_then(|opt| opt.get_string().ok())
                .cloned();

            Ok(TrackerResponse {
                interval_seconds,
                tracker_id,
                seeders,
                leechers,
                peers,
                warning_message,
            })
        }
    }
}
