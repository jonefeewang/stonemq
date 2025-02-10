# Stonemq

[![License](https://img.shields.io/badge/license-Apache%202-blue)](LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/username/stonemq/ci.yml)](https://github.com/username/stonemq/actions)
[![Crates.io](https://img.shields.io/crates/v/stonemq)](https://crates.io/crates/stonemq)

Stonemq aims to outperform Kafka in scenarios with massive-scale queue clusters, delivering enhanced performance and efficiency to achieve cost reduction and operational optimization.

In use cases involving large clusters with countless queues—particularly in enterprise business services and public cloud services—there can be tens of thousands of partitions and partition leaders. Regardless of whether the queues contain messages, the volume of messages, or the flow rate, the presence or absence of active consumers in each partition poses a significant burden for cluster operators. Partition growth negatively impacts cluster throughput, while node failures or restarts often result in partition leader and controller switching, creating critical operational challenges. Stonemq addresses these inefficiencies.

Cluster performance should remain consistent regardless of partition growth. Queues with varying traffic volumes need consolidation to enable more efficient message flow—akin to containerized shipping for multiple clients. This is precisely the vision of Stonemq. While solutions like Pulsar utilize journaling for centralized message handling, Stonemq seeks to retain Kafka’s replication , which simplifies and standardizes cluster operation and maintenance.We believe this protocol is both straightforward and highly efficient, forming the backbone of our solution.Additionally, by reusing Kafka's client-server communication protocol, StoneMQ ensures seamless migration without requiring any changes to the user's client applications. This approach significantly reduces adoption costs for users, enabling a smooth transition to StoneMQ while retaining the familiar and reliable interface they are accustomed to. 

---

## Current Status

- version 0.1.0 released 
- Supports single-node message sending and receiving.  
- Implements group consumption functionality.  

---

## Installation and Usage

### Build From Source

1. Make sure you have Rust installed (recommended via [Rustup](https://rustup.rs/)):

   ```bash
   rustc --version
   ```

2. Clone the repository:

   ```bash
   git clone https://github.com/jonefeewang/stonemq.git
   cd stonemq
   ```

3. Build the release version using Cargo:

   ```bash
   cargo build --release
   ```

4. Run the application:

   ```bash
   ./target/release/stonemq
   ```

## **Quick Start**

### **1. Start the StoneMQ Server**  

Run the following command to launch the StoneMQ server:  

```bash
./stonemq
```

---

### **2. Connect to StoneMQ**  

Use a **Kafka client library** (or REST API) with a minimum supported version of **Kafka v0.11** to connect to the StoneMQ server for publishing and subscribing to messages. StoneMQ uses the same default server port as Kafka: **9092**. This port can be customized in the configuration file.

---

### **3. Publish a Message**  

StoneMQ comes with a built-in topic, **`topic_a`**, which is pre-configured with **2 partitions**:  

- `topic_a-0`  
- `topic_a-1`  

By default, the system sets up with **2 journals**.

---

### **4. Configuration**  

The configuration file, **`conf.toml`**, is located in the project’s root directory. Adjust settings as needed to customize the server's behavior.

---

## Development Guide

### Local Development Setup

1. **Install Rust Toolchain**:
   Ensure Rust is installed on your system. Use [Rustup](https://rustup.rs/) to get started:

   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   rustup default stable
   ```

2. **Clone the Repository**:

   ```bash
   git clone https://github.com/username/stonemq.git
   cd stonemq
   ```

3. **Run Tests**:
   Run all unit tests to ensure everything is working correctly:

   ```bash
   cargo test
   ```

4. **Check Code Formatting**:
   Ensure the code adheres to the formatting rules:

   ```bash
   cargo fmt -- --check
   ```

5. **Lint (Static Analysis)**:

   Run clippy to check for potential issues:

   ```bash
   cargo clippy
   ```

6. **Log Parsing**  

   An executable file, **`log_parser.rs`**, is available in the project's `bin` directory. This tool can be used to parse `log`,`index`,and `checkpoint` files.

7. **Benchmarking and Monitoring**  

   The entry file **`stonemq.rs`** in the `bin` directory allows you to specify the current running mode: **prod**, **dev**, or **perf**. The default 	mode is **prod**.  

   - **Dev Mode**: Suitable for development.  
   - **Perf Mode**: Enables the use of **tokio-console** to monitor the state of Tokio tasks within the process.  
     Additionally, you can specify whether to enable **trace**. When enabled, added spans can be observed in **Open Jaeger** for in-depth monitoring.

---

## Contribution Guide

Contributions are welcome and greatly appreciated! Whether it's reporting a bug, submitting code, improving documentation, or suggesting ideas, your help is valuable.

### Steps to Contribute

1. Fork the repository and create a new branch for your feature/bugfix.
2. Ensure all tests pass, and your code adheres to the project's standards.
3. Submit a pull request with a clear description of your changes.

---

## Community Support

If you encounter issues or have questions, feel free to reach out to the community:

- 🐛 Submit Issues: [GitHub Issues](https://github.com/username/stonemq/issues)
- 📢 Join Discussions: [GitHub Discussions](https://github.com/username/stonemq/discussions)

---

## License

Stonemq is distributed under the **Apache License 2.0**. See the [LICENSE](./LICENSE) file for more details.

