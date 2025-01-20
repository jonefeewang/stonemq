// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
