#!/bin/bash

apt update && apt install -y \
  build-essential cmake git \
  libgmp3-dev libprocps-dev python3-markdown libboost-program-options-dev \
  libssl-dev python3 pkg-config libgtest-dev