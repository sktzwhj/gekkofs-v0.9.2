#!/bin/bash

make CFLAGS=-Wno-error -j install
rsync -av /usr/local/ucx-mt/ 12.0.2.0:/usr/local/ucx-mt

