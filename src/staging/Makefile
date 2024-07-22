CC = mpic++
INCLUDEDIR = -I/fs2/home/wuhuijun/lmj/source/gekkofs-v0.9.2/include -I/fs2/home/wuhuijun/lmj/source/gekkofs-v0.9.2/build/include/

STAGE_IN_PROGRAM = stage-in
STAGE_OUT_PROGRAM = stage-out
STAGE_IN_SOURCE = ./src/stage-in.cpp
STAGE_OUT_SOURCE = ./src/stage-out.cpp
GO_PROGRAM = tools

build-cpp:
	$(CC) $(CFLAGS) -o $(STAGE_IN_PROGRAM) $(INCLUDEDIR) $(STAGE_IN_SOURCE) -lpthread
	$(CC) $(CFLAGS) -o $(STAGE_OUT_PROGRAM) $(INCLUDEDIR) $(STAGE_OUT_SOURCE) -lpthread

build-go:
	go build  -o $(GO_PROGRAM)

make clean:
	rm -f $(STAGE_IN_PROGRAM) $(STAGE_OUT_PROGRAM) $(GO_PROGRAM) gkfs_hosts.txt*

make all: clean build-go build-cpp