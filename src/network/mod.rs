//! Network Module Implementation
//!
//! This module provides the core networking functionality for the messaging system,
//! handling TCP connections, frame parsing, and request processing.
//!
//! # Architecture
//!
//! The network module is built on tokio's async I/O primitives and consists of:
//! - Connection management for handling TCP connections
//! - Frame parsing for processing network packets
//! - Request handling for processing client requests
//!
//! # Components
//!
//! - `Connection`: Manages individual TCP connections
//! - `RequestFrame`: Handles parsing and validation of network frames
//!
//! # Features
//!
//! - Asynchronous I/O operations
//! - Buffer management for efficient memory usage
//! - Frame size validation
//! - Connection tracking
//! - Error handling for network operations

pub use connection::Connection;
pub use frame::RequestFrame;
mod connection;
mod frame;
