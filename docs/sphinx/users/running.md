# Running GekkoFS

This section describes how to run GekkoFS locally or within a cluster environment.

## General

First of all, the GekkoFS daemon (`gkfs_daemon` binary) has to be started on each node. Other tools can be used to
execute the binary on many nodes, e.g., `srun`,
`mpiexec/mpirun`, `pdsh`, or `pssh`.

Now, you need to decide what Mercury NA plugin you want to use for network communication.
`ofi+sockets` is the default and uses the TCP protocol.

When running the daemon binary, the `-P` argument can be used to control which RPC protocol should be used:

- `ofi+sockets` for using the libfabric plugin with TCP (stable)
- `ofi+tcp` for using the libfabric plugin with TCP (slower than sockets)
- `ofi+verbs` for using the libfabric plugin with Infiniband verbs (reasonably stable) and requires
  the [rdma-core (formerly libibverbs)](https://github.com/linux-rdma/rdma-core) library
- `ofi+psm2` for using the libfabric plugin with Intel Omni-Path (unstable) and requires
  the [opa-psm2](https://github.com/cornelisnetworks/opa-psm2>) library

## The GekkoFS hostsfile

Each GekkoFS daemon needs to register itself in a shared file (*host file*) which needs to be accessible to **all**
GekkoFS clients and daemons. Therefore, the hostsfile describes a file system and which node is part of that specific
GekkoFS file system instance. Conceptually, the hostsfile represents a single GekkoFS file system instance. That is to
say, all daemons of one GekkoFS use the same hostsfile to identify as a server in that file system instance and
namespace.

```{important}
At this time, we only support strongly consistent parallel file systems, such as Lustre or GPFS, for storing the hostsfile, when one GekkoFS file systems consists of multiple servers.
While we will offer an alternative in the future, this means that eventual consistent file systems, e.g., NFS, cannot be used for the hostsfile.
Note that if only one daemon is part of a file system instance, and all GekkoFS client are run on the same node (e.g., a laptop), the hostsfile can be stored on a local file system.
```

## GekkoFS daemon start and shutdown

tl;dr example: `<install_path>/bin/gkfs_daemon -r <fs_data_path> -m <pseudo_gkfs_mount_dir_path> -H <hostsfile_path>`

When running the daemon, it requires two mandatory arguments: which specify where the daemon stores its data and
metadata locally, and at which path clients can access the file system (mount point):

1. `-r/--rootdir <fs_data_path>` specifies where the daemon stores its data and metadata locally. In general, the daemon
   can use any device with a file system path that is accessible to the user, e.g., a RAMDisk or a node-local SSD.

2. `-m/--mountdir <pseudo_gkfs_mount_dir_path>` specifies a pseudo mount directory used by clients to access GekkoFS.
   This pseudo mount directory differs from usual file system mount points which mounted as a kernel-based file system.
   Therefore, GekkoFS will **not** appear when typing `mount` on the command line. Rather, the pseudo mount dir is used
   later by the client interposition library which intercepts file system operations and processes those which are
   within the GekkoFS namespace.

3. (optional) `-H/--hosts-file <hostsfile_path>` specifies the path where the hostsfile is placed. In a distributed
   environment, all daemons should use the same file (see above) and, therefore, this argument should be used. By
   default, the daemon creates a hostsfile in the current working directory (see below).

Further options are available

```bash
Allowed options
Usage: src/daemon/gkfs_daemon [OPTIONS]

Options:
  -h,--help                   Print this help message and exit
  -m,--mountdir TEXT REQUIRED Virtual mounting directory where GekkoFS is available.
  -r,--rootdir TEXT REQUIRED  Local data directory where GekkoFS data for this daemon is stored.
  -s,--rootdir-suffix TEXT    Creates an additional directory within the rootdir, allowing multiple daemons on one node.
  -i,--metadir TEXT           Metadata directory where GekkoFS RocksDB data directory is located. If not set, rootdir is used.
  -l,--listen TEXT            Address or interface to bind the daemon to. Default: local hostname.
                              When used with ofi+verbs the FI_VERBS_IFACE environment variable is set accordingly which associates the verbs device with the network interface. In case FI_VERBS_IFACE is already defined, the argument is ignored. Default 'ib'.
  -H,--hosts-file TEXT        Shared file used by deamons to register their endpoints. (default './gkfs_hosts.txt')
  -P,--rpc-protocol TEXT      Used RPC protocol for inter-node communication.
                              Available: {ofi+sockets, ofi+verbs, ofi+psm2} for TCP, Infiniband, and Omni-Path, respectively. (Default ofi+sockets)
                              Libfabric must have enabled support verbs or psm2.
  --auto-sm                   Enables intra-node communication (IPCs) via the `na+sm` (shared memory) protocol, instead of using the RPC protocol. (Default off)
  --clean-rootdir             Cleans Rootdir >before< launching the deamon
  -c,--clean-rootdir-finish   Cleans Rootdir >after< the deamon finishes
  -d,--dbbackend TEXT         Metadata database backend to use. Available: {rocksdb, parallaxdb}
                              RocksDB is default if not set. Parallax support is experimental.
                              Note, parallaxdb creates a file called rocksdbx with 8GB created in metadir.
  --parallaxsize TEXT         parallaxdb - metadata file size in GB (default 8GB), used only with new files
  --enable-collection         Enables collection of general statistics. Output requires either the --output-stats or --enable-prometheus argument.
  --enable-chunkstats         Enables collection of data chunk statistics in I/O operations.Output requires either the --output-stats or --enable-prometheus argument.
  --output-stats TEXT         Creates a thread that outputs the server stats each 10s to the specified file.
  --enable-prometheus         Enables prometheus output and a corresponding thread.
  --prometheus-gateway TEXT   Defines the prometheus gateway <ip:port> (Default 127.0.0.1:9091).
  --version                   Print version and exit.
````

Shut it down by gracefully killing the process (SIGTERM).

```{note}
It is possible to run multiple independent GekkoFS instances on the same node. Note, that when these GekkoFS instances
are part of the same file system, use the same `rootdir` with different `rootdir-suffixe`.
```

### Running and shutting down GekkoFS as a Slurm job step

To run GekkoFS as a Slurm job step, it is as easy as executing `srun <path_to_daemon_binary>/gkfs_daemon <arguments> &`
to launch GekkoFS in the background. An easy way to check if all daemons have started, is to count the number of lines
in the hostsfile which corresponds to the number of started daemons.

It is recommended that you explicitly tell Slurm which and how many resources it should use on each node.
Noteworthy `srun` arguments are as follows:

- `--disable-status` allows to immediately forward a signal, e.g., `SIGINT`, to the running job
- `-N`, `--ntasks`, and `--ntasks-per-node` allows defining how many daemons are run on how many nodes
- `--cpus-per-task` allows to set the number of CPU resources the daemon can use

If a node features multiple CPU sockets, it is recommended to pin the daemon to one socket with `numactl`, `taskset`, or
similar.

To gracefully shut down all daemons, send `SIGINT` to the background process: `kill -s SIGINT <srun_gekkofs_pid>`. This
command finishes once all daemons have shut down.

## Use the GekkoFS client library

tl;dr example:

```bash
export LIBGKFS_ HOSTS_FILE=<hostfile_path>
LD_PRELOAD=<install_path>/lib64/libgkfs_intercept.so cp ~/some_input_data <pseudo_gkfs_mount_dir_path>/some_input_data
LD_PRELOAD=<install_path>/lib64/libgkfs_intercept.so md5sum ~/some_input_data <pseudo_gkfs_mount_dir_path>/some_input_data
```

Clients read the hostsfile to determine which daemons are part of the GekkoFS instance. Because the client is an
interposition library that is loaded within the context of the application, this information is passed via the
environment variable `LIBGKFS_HOSTS_FILE` pointing to the hostsfile path. The client library itself is loaded for each
application process via the `LD_PRELOAD` environment variable intercepting file system related calls. If they are
within (or hierarchically under) the GekkoFS mount directory they are processed in the library, otherwise they are
passed to the kernel.

Note, if `LD_PRELOAD` is not pointing to the library and, hence the client is not loaded, the mounting directory appears
to be empty.

For MPI applications, the `LD_PRELOAD` and `LIBGKFS_HOSTS_FILE` variables can be passed with the `-x` argument
for `mpirun/mpiexec`.

## Run GekkoFS daemons on multiple nodes (beta version!)

The `scripts/run/gkfs` script can be used to simplify starting the GekkoFS daemon on one or multiple nodes. To start
GekkoFS on multiple nodes, a Slurm environment that can execute `srun` is required. Users can further
modify `scripts/run/gkfs.conf` to mold default configurations to their environment.

The following options are available for `scripts/run/gkfs`:

```bash
usage: gkfs [-h/--help] [-r/--rootdir <path>] [-m/--mountdir <path>] [-a/--args <daemon_args>] [-f/--foreground <false>]
        [--srun <false>] [-n/--numnodes <jobsize>] [--cpuspertask <64>] [--numactl <false>] [-v/--verbose <false>]
        {start,stop}


    This script simplifies the starting and stopping GekkoFS daemons. If daemons are started on multiple nodes,
    a Slurm environment is required. The script looks for the 'gkfs.conf' file in the same directory where
    additional permanent configurations can be set.

    positional arguments:
            command                 Command to execute: 'start' and 'stop'

    optional arguments:
            -h, --help              Shows this help message and exits
            -r, --rootdir <path>    Providing the rootdir path for GekkoFS daemons.
            -m, --mountdir <path>   Providing the mountdir path for GekkoFS daemons.
            -a, --args <daemon_arguments>
                                    Add various additional daemon arguments, e.g., "-l ib0 -P ofi+psm2".
            -f, --foreground        Starts the script in the foreground. Daemons are stopped by pressing 'q'.
            --srun                  Use srun to start daemons on multiple nodes.
            -n, --numnodes <n>      GekkoFS daemons are started on n nodes.
                                    Nodelist is extracted from Slurm via the SLURM_JOB_ID env variable.
            --cpuspertask <#cores>  Set the number of cores the daemons can use. Must use '--srun'.
            --numactl               Use numactl for the daemon. Modify gkfs.conf for further numactl configurations.
            -c, --config            Path to configuration file. By defaults looks for a 'gkfs.conf' in this directory.
            -v, --verbose           Increase verbosity
```

### Logging

#### Client logging

The following environment variables can be used to enable logging in the client library: `LIBGKFS_LOG=<module>`
and `LIBGKFS_LOG_OUTPUT=<path/to/file>` to configure the output module and set the path to the log file of the client
library. If not path is specified in `LIBGKFS_LOG_OUTPUT`, the client library will send log messages
to `/tmp/gkfs_client.log`. With `LIBGKFS_LOG_PER_PROCESS=1` it is possible to write separate logs per client process.
If so, the path specified in `LIBGKFS_LOG_OUTPUT` is a directory but should not end with a `/`.

The following modules are available:

- `none`: don't print any messages
- `syscalls`: Trace system calls: print the name of each system call, its arguments, and its return value. All system
  calls are printed after being executed save for those that may not return, such as `execve()`,
  `execve_at()`, `exit()`, and `exit_group()`. This module will only be available if the client library is built
  in `Debug` mode.
- `syscalls_at_entry`: Trace system calls: print the name of each system call and its arguments. All system calls are
  printed before being executed and therefore their return values are not available in the log. This module will only be
  available if the client library is built in `Debug` mode.
- `info`: Print information messages.
- `critical`: Print critical errors.
- `errors`: Print errors.
- `warnings`: Print warnings.
- `mercury`: Print Mercury messages.
- `debug`: Print debug messages. This module will only be available if the client library is built in `Debug` mode.
- `most`: All previous options combined except `syscalls_at_entry`. This module will only be available if the client
  library is built in `Debug`
  mode.
- `all`: All previous options combined.
- `trace_reads`: Generate log line with extra information in read operations for guided distributor
- `help`: Print a help message and exit.

When tracing sytem calls, specific syscalls can be removed from log messages by setting the `LIBGKFS_LOG_SYSCALL_FILTER`
environment variable. For instance, setting it to `LIBGKFS_LOG_SYSCALL_FILTER=epoll_wait,epoll_create` will filter out
any log entries from the `epoll_wait()` and `epoll_create()` system calls.

Additionally, setting the `LIBGKFS_LOG_OUTPUT_TRUNC` environment variable with a value different from `0` will instruct
the logging subsystem to truncate the file used for logging, rather than append to it.

#### Daemon logging

For the daemon, the `GKFS_DAEMON_LOG_PATH=<path/to/file>` environment variable can be provided to set the path to the
log file, and the log module can be selected with the `GKFS_DAEMON_LOG_LEVEL={off,critical,err,warn,info,debug,trace}`
environment variable whereas `trace` produces the most trace records while `info` is the default value.
