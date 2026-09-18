# Reliable Transport Protocol over UDP

This repository contains the implementation of a custom transport-layer protocol running over UDP for the second assignment of the Communication Networks course. The project demonstrates core networking functions including a three-way handshake for multi-client connection setup, Selective Repeat ARQ for flow control, out-of-order packet management, and non-blocking I/O multiplexing.

---

## Features & Implementation Details

### Module 1: Three-Way Handshake & Multi-Client Support

To ensure reliable connection establishment and prevent concurrency bottlenecks on the main server, a multi-socket handshake mechanism was implemented:

* **Handshake Workflow & Retransmissions:** The client initiates the connection by sending a `SYN` packet to the main server port (`8032`) and waits for a response. A timer handles potential `SYN` loss; if it expires, the client retransmits the `SYN`.
* **Server Duplication Handling:** The server listens on port `8032` and stores client source ports in a `std::map`. Duplicate `SYN` packets resulting from network latency are identified via the map and safely ignored to prevent deadlocks or redundant allocations.
* **Dynamic Port Allocation:** To handle multiple concurrent clients, the server creates a **dedicated socket** on an available OS-assigned random port for each accepted connection. The `SYN-ACK` payload contains this newly allocated port.
* **Handshake Finalization:** The client extracts the new port and connection ID, updates its destination socket target, and transmits the final `ACK`. To mitigate UDP packet loss during handshakes, the final `ACK` is sent multiple times.

---

### Module 2: Selective Repeat ARQ & Sliding Window

File transfers rely on an optimized sliding window algorithm to guarantee reliable, ordered delivery across lossy channels:

* **Circular Transmission Buffer:** Data read from files is continuously fed into a circular buffer for segment formulation.
* **Window Flow Control:** When the number of unacknowledged packets reaches the window limit, the application/sender thread pauses until window space is freed by incoming `ACK`s.
* **Selective Timeout Retransmission:** To prevent network congestion during packet loss events, the sender does not retransmit the entire window on timeout. Instead, it selectively retransmits only the last 4 unacknowledged packets.
* **Window Advancement:** Upon receiving a valid `ACK`, all packets with sequence numbers lower than or equal to the acknowledged sequence are removed from the window, sliding it forward to accept new segments.

---

### Module 3: Flow Control, Out-of-Order Handling & I/O Multiplexing

The receiver (server) handles incoming data through non-blocking multiplexed I/O and strict buffer management:

* **I/O Multiplexing:** The server handles both the primary listening socket and all active client sockets within a single event loop using non-blocking I/O multiplexing, avoiding high-CPU busy waiting.
* **Out-of-Order Packet Management:** Packets arriving out of order are cached in a secondary structure. Once missing sequence gaps are filled, the server reconstructs contiguous packet ranges in order, writes them to disk, and flushes them from the cache.
* **Flow Control & Integrity:** The server maintains a dedicated receiver buffer for the application layer. When space becomes available, remaining data is shifted left toward the start of the buffer. If the buffer is full, incoming packets are dropped.
* **Receiver Window Feedback:** Every `ACK` sent back to the client contains the current remaining receive buffer capacity (`RECV_WINDOW`).