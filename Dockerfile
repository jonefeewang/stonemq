# 使用阿里云的 Ubuntu 源（以 Ubuntu 20.04 为例）
# 使用 Ubuntu 作为基础镜像
FROM ubuntu:20.04

RUN sed -i 's|http://archive.ubuntu.com/ubuntu/|https://mirrors.aliyun.com/ubuntu/|g' /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y \
    build-essential \
    rustc \
    cargo \
    heaptrack \
    htop \
    clang \
    libclang-dev \
    && apt-get clean

# 设置 LIBCLANG_PATH 环境变量
ENV LIBCLANG_PATH=/usr/lib/llvm-10/lib/

# 创建 /tmp/queue 和 /tmp/journal 目录
RUN mkdir -p /tmp/queue /tmp/journal /usr/src/stonemq/stonemq_perf



# 设置工作目录
WORKDIR /usr/src/stonemq

# 将当前目录（主机）中的所有文件复制到容器内部的工作目录
COPY . .

# 构建 Rust 项目
# RUN cargo build --release

# 默认命令（在容器中运行你的可执行文件）
# CMD ["heaptrack", "./target/release/stonemq"]
