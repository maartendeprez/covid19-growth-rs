#!/bin/bash

OPENSSL_DIR=/home/maarten/Dokumentoj/werf/openssl/openssl-1.1.1g TARGET_CC=armv7l-linux-musleabihf-gcc TARGET_CFLAGS=-march=armv7-a+neon-vfpv4 cargo build --release --target=armv7-unknown-linux-musleabihf
scp target/armv7-unknown-linux-musleabihf/release/covid19-growth-rs crissaegrim.be.eu.org:/tmp/covid19/
ssh crissaegrim.be.eu.org cd /tmp/covid19/\; ./covid19-growth-rs
