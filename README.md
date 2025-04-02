# AhriMQ (AMQ)

[![Build Status](https://github.com/ahriroot/ahrimq/actions/workflows/release.yml/badge.svg)](https://github.com/ahriroot/ahrimq/actions)
[![GitHub Release](https://img.shields.io/github/v/release/ahriroot/ahrimq?style=flat-square)](https://github.com/ahriroot/ahrimq/releases)
[![License](https://img.shields.io/github/license/ahriroot/ahrimq?style=flat-square)](https://github.com/ahriroot/ahrimq)

> A high-performance message queue service supporting pub/sub, ordered messages, delayed messages and dead letter queues.

## Install from Crates.io

```bash
cargo install ahrimq
```

## Install from Source

```bash
git clone https://github.com/ahriroot/ahrimq.git
cd ahrimq
cargo build --release
```

## Features

- Sub and Pub: Support for publishing and subscribing to messages.
- Normal Messages: Support for normal messages.
- Ordered Messages: Support for ordered messages.
- Delay Messages: Support for delayed messages.
- Dead Letter Queues: Support for dead letter queues.

## License

MIT
