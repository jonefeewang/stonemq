# use aliyun ubuntu source
# use ubuntu 20.04 as base image
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

# set LIBCLANG_PATH environment variable
ENV LIBCLANG_PATH=/usr/lib/llvm-10/lib/

# create /tmp/queue and /tmp/journal directory
RUN mkdir -p /tmp/queue /tmp/journal /usr/src/stonemq/stonemq_perf



# set working directory
WORKDIR /usr/src/stonemq

# copy all files from host to container working directory
COPY . .

# build rust project
RUN cargo build --release

# default command (run your executable file in container)
CMD ["heaptrack", "./target/release/stonemq"]
