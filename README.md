# Bitcrystal

Experiment in implementing a Bittorrent client.

Not fully featured, supports single file via torrent files only, and quite certainly not very efficient.

To-do list if come back to this in the future:

- Multi file torrents
- Magnet links
- User interface
- Multiple torrents
- and more...

## Code structure

Spin up Tracker, Worker, Listener tasks, where main will continue to spin up Peer tasks as needed.

Tracker task periodically calls tracker URL, and on events (completed/stopped), to update peers vector.

Worker maintains state of file being downloaded, dishing out work pieces on request, handling requests for data, and handling data coming in and writing verified pieces to disk.

Listener waits for connections, and spins up peer tasks to maintain a constant amount of connections.

Peer represents a single connection with a peer. Handles maintaining state of connection, getting work from worker, and communicating via TCP with peer.

Communication between tasks is async, using channels of various forms.
