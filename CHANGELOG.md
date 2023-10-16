# Changelog

All notable changes to GekkoFS project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### New

- Additional tests to increase code coverage ([!141](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/141)).
- GKFS_ENABLE_UNUSED_FUNCTIONS added to disable code to increase code
  coverage. ([!141](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/141)).
- Updated Parallax version to new API (parallax option needs kv_format.parallax in the path, and the database in a
  device with O_DIRECT) ([!158](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/158)
- Support for increasing file size via `truncate()`
  added ([!159](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/159)
- Added PowerPC support ([!151](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/151)).
- GKFS_RENAME_SUPPORT added to support renaming files. This specifically targets the use case for opened files using an
  existing file descriptor ([!133](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/133)).
- Added `FLOCK` and `fcntl` functions for locks to interception albeit not supported by GekkoFS and returning the
  corresponding error code ([!133](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/133)).
- Added ARM64 support ([!160](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/160)).
- Added support for [CMake presets](https://cmake.org/cmake/help/latest/manual/cmake-presets.7.html) to simplify build 
  configurations ([!163](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/163#note_8179)).
- Several improvements to CMake scripts ([!143](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/143))):
  - Dependency management is now handled more consistently: system
    dependencies are found using `find_package()`, whereas source-only
    dependencies are found using `include_from_source()`. This new function
    integrates a dependency provided its source code is available at
    `GKFS_DEPENDENCIES_PATH`. If it's not, it will try to
    automatically download it from its git repository using CMake's
    `FetchContent()`.
  - More consistent use of targets (we are closer to 100% modern CMake).
  - Adds the `gkfs_feature_summary()` to allow printing a summary of all
    GekkoFS configuration options and their values. This should help users
    when building to precisely see how a GekkoFS instance has been configured.
- Added (parallel) append support for consecutive writes with file descriptor opened
  with `O_APPEND` ([!164](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/164)).
- Added support for Spack so that it can be used to install
  GekkoFS ([!137](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/137)).

### Changed

- Support parallelism for path resolution
  tests ([!145](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/145)).
- Support parallelism for symlink tests ([!147](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/147)).
- Update Parallax release (PARALLAX-exp) ([!158](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/158)
- Improved and simplified coverage generation procedures for developers with
  specific CMake targets ([!163](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/163#note_8179)).

### Removed

### Fixed

- Updated daemon log level for tests ([!138](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/138)).
- Using `unlink` now fails if it is a directory unless the `AT_REMOVEDIR` flag is used (POSIX
  compliance) ([!139](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/139)).
- fchdir generate a SIGSEV in debug mode (due to
  log) ([!141](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/141))
- Support glibc-2.34 or newer with
  syscall_intercept [!146](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/146)).
- Fixed segfault in sfind/gfind ([!148](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/148)
- Fixed fstatat to be able to understand `AT_EMPTY_PATH` flag used in coreutils (`cat`
  ...) ([!149](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/149)).
- Updated CI pipeline for Gitlab version 15.1.1 ([!150](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/150))
  .
- Fixed a compilation error for testing ([!153](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/153)).
- Fixed a bug which caused GekkoFS tests to
  fail ([!154](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/154)).
- Fixed RocksDB compilation for future GCC
  versions ([!155](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/155)).
- Fixed an issue where GekkoFS would halt when applications would create child processes, e.g., with fork, clone,
  pthread_create ([!156](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/156)
- Fixed an issue where compilation of syscall_intercept would fail for newer kernels using the `clone3()` system
  call ([!157](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/157)
- Updated code formatter to support the most recent clang-format-15
  version ([!162](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/162))
- Fixed an issue where nlohmann json failed to download in
  CMake ([!167](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/167)).
- Refactored update file size during write
  operations ([!164](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/164)).

## [0.9.1] - 2022-04-29

### New

- Added a new script for starting and stopping daemons on multiple
  nodes ([!135](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/135)).
- Added statistics gathering on daemons ([!132](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/132)).
    - Stats output can be enabled with:
    - `--enable-collection` collects normal statistics.
    - `--enable-chunkstats` collects extended chunk statistics.
- Statistics output to file is controlled by `--output-stats <filename>`
- Added Prometheus support for outputting
  statistics ([!132](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/132)):
  - Prometheus dependency optional and enabled at compile time with the CMake argument `GKFS_ENABLE_PROMETHEUS`.
  - `--enable-prometheus` enables statistics pushing to Prometheus if statistics are enabled.
  - `--prometheus-gateway` sets an IP and port for the Prometheus connection.
- Added new experimental metadata backend:
  Parallax ([!110](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/110)).
  - Added support to use multiple metadata backends.
  - Added `--clean-rootdir-finish` argument to remove rootdir/metadir at the end when the daemon finishes.

### Changed

- `-c` argument has been moved to `--clean-rootdir-finish` and is now used to clean rootdir/metadir on daemon
  shutdown ([!110](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/110)).
- Environment variable to change Daemon log levels was changed from `GKFS_LOG_LEVEL`
  to `GKFS_DAEMON_LOG_LEVEL` ([!135](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/135)).
- Update Catch2 to support newer glibc library ([!131](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/131)).
- Adding support for `faccessat2()` and `getxattr()` system
  calls ([!209](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/209)).

### Removed

- Removed old initialization code in the GekkoFS
  client ([!136](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/136)).
- Removed boost interval dependencies from guided
  distributor ([!122](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/122)).

### Fixed

- Documentation: Doxygen now includes private struct and class
  members ([!125](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/125)).
- Guided distributor tests are no longer run when they are turned
  off ([!129](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/129)).

## [0.9.0] - 2022-02-22

### New

- GekkoFS now uses C++17 (!74).
- Added a new `dirents_extended` function which can improve `find` operations. A corresponding example how to use this
  function can be found at
  `examples/gfind/gfind.cpp` with a non-mpi version at `examples/gfind/sfind.cpp`
  ([!73](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/73)).
- Code coverage reports for the source code are now generated and tracked
  ([!77](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/77)).
- Considerable overhaul and new features of the GekkoFS testing facilities
  ([!120](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/120),
  [!121](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/121)).
- Namespace have been added to the complete GekkoFS codebase
  ([!82](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/82)).
- The system call `socketcall()` is now supported
  ([!91](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/91)).
- System call error codes are now checked in `syscall_no_intercept`
  scenarios in non x86 architectures (![!92](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/92)).
- GekkoFS documentation is now automatically generated and published
  at [here](https://storage.bsc.es/projects/gekkofs/documentation/index.html)
  ([!95](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/95),
  [!109](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/109),
  [!125](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/125)).
- Added a guided distributor mode which allows defining a specific distribution of data
  on a per directory or file basis ([!39](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/39)).
- For developers:
  - A convenience library has been added for unit testing
    ([!94](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/94)).
  - Code format is now enforced with the `clang-format` tool
    ([!66](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/66)).
    A new script is available in `scripts/check_format.sh` for easy of use.
  - `GKFS_METADATA_MOD` macro has been added allowing the MetadataModule to be
    logged, among others ([!98](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/98)).
  - A convenience library has been added for `path_util`
    ([!102](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/102)).

### Changed

- GekkoFS license has been changed to GNU General Public License version 3
  ([!88](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/88))
- Create, stat, and remove operation have been refactored and improved, reducing
  the number of required RPCs per operation ([!60](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/60)).
- Syscall_intercept now supports glibc version 2.3 or newer
  ([!72](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/72)).
- All arithmetic operations based on block sizes, and therefore chunk computations,
  are now `constexpr` ([!75](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/75)).
- The CI pipeline has been significantly optimized
  ([!103](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/103)).
- The GekkoFS dependency download and compile scripts have been severely refactored
  and improved ([!111](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/111)).
- GekkoFS now supports the latest dependency versions
  ([!112](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/112)).

### Removed

- Boost is no longer used for the client and
  daemon ([!90](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/90),
  [!122](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/122),
  [!123](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/123)).
  Note that tests still require `Boost_preprocessor`.
- Unneeded sources in CMake have been removed ([!101](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/101)).

### Fixed

- Building tests no longer proceeds if virtualenv creation
  fails ([!68](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/68)).
- An error where unit tests could not be found has been
  fixed ([!79](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/79)).
- The daemon can now be restarted without losing its
  namespace ([!85](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/85)).
- An issue has been resolved that required AGIOS even if it wasn't been
  used ([!104](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/104)).
- Several issues that caused docker images to fail has been
  resolved ([!105](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/105),
  [!106](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/106)
  , [!107](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/107),
  [!114](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/114)).
- An CMake issue in `path_util` that caused the compilation to fail was
  fixed ([!115](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/115)).
- Fixed an issue where `ls` failed because newer kernels use `fstatat()` with `EMPTY_PATH`
  ([!116](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/116)).
- Fixed an issue where `LOG_OUTPUT_TRUNC` did not work as
  expected ([!118](https://storage.bsc.es/gitlab/hpc/gekkofs/-/merge_requests/118)).

## [0.8.0] - 2020-09-15

### New

- Both client library and daemon have been extended to support the ofi+verbs
  protocol.
- A new Python testing harness has been implemented to support integration
  tests. The end goal is to increase the robustness of the code in the mid- to
  long-term.
- The RPC protocol and the usage of shared memory for intra-node communication
  no longer need to be activated on compile time. New arguments
  `-P|--rpc-protocol` and `--auto-sm` have been added to the daemon to this
  effect. This configuration options are propagated to clients when they
  initialize and contact daemons.
- Native support for the Omni-Path network protocol by choosing the `ofi+psm2`
  RPC protocol. Note that this requires `libfabric`'s version to be greater
  than `1.8` as well as `psm2` to be installed in the system. Clients must set
  `FI_PSM2_DISCONNECT=1` to be able to reconnect once the client is shut down
  once.
  *Known limitations:* Client reconnect doesn't always work. Apparently, if
  clients reconnect too fast the servers won't accept the connections. Also,
  currently more than 16 clients per node are not supported.
- A new execution mode called `GekkoFWD` that allows GekkoFS to run as
  a user-level I/O forwarding infrastructure for applications. In this mode,
  I/O operations from an application are intercepted and forwarded to a single
  GekkoFS daemon that is chosen according to a pre-defined distribution. In the
  daemons, the requests are scheduled using the AGIOS scheduling library before
  they are dispatched to the shared backend parallel file system.
- The `fsync()` system call is now fully supported.

### Improved

- Argobots tasks in the daemon are now wrapped in a dedicated class,
  effectively removing the dependency. This lays ground work for future
  non-Argobots I/O implementations.
- The `readdir()` implementation has been refactored and improved.
- Improvements on how to the installation scripts manage dependencies.

### Fixed

- The server sometimes crashed due to uncaught system errors in the storage
  backend. This has now been fixed.
- Fixed a bug that broke `ls` on some architectures.
- Fixed a bug that leaked internal errors from the interception library to
  client applications via `errno` propagation.

## [0.7.0] - 2020-02-05

### Added

- Added support for `eventfd()`and `eventfd2()` system calls.

### Changed

- Replaced Margo with Mercury in the client library in order to increase
  application compatibility: the Argobots ULTs used by Margo to send and
  process RPCs clashed at times with applications using pthreads.
- Renamed environment variables to better distinguish which variables affect
  the client library (`LIBGKFS_*`) and which affect the daemon
  (`GKFS_DAEMON_*`).
- Replaced spdlog in the client with a bespoke logging infrastructure:
  spdlog's internal threads and exception management often had issues with the
  system call interception infrastructure. The current logging infrastructure
  is designed around the syscall interception mechanism, and is therefore more
  stable.
- Due to the new logging infrastructure, there have been significant changes
  to the environment variables controlling logging output. The desired log
  module is now set with `LIBGKFS_LOG`, while the desired output channel is
  controlled with `LIBGKFS_LOG_OUTPUT`. Additional options such as
  `LIBGKFS_LOG_OUTPUT_TRUNC`, `LOG_SYSCALL_FILTER` and `LOG_DEBUG_VERBOSITY`
  can be used to further control messages. Run the client with
  `LIBGKFS_LOG=help` for more details.
- Improved dependency management in CMake.

### Fixed

- Relocate internal file descriptors to a private range to avoid interfering
  with client application file descriptors.
- Handle internal file descriptors created by `fcntl()`.
- Handle internal file descriptors passed to processes using `CMSG_DATA` in
  `recvmsg()`.

## [0.6.2] - 2019-10-07

### Added

- Paths inside kernel pseudo filesystems (`/sys`, `/proc`) are forwarded directly to the kernel and internal path
  resolution will be skipped. Be aware that also paths like  `/sys/../tmp/gkfs_mountpoint/asd` will be forwarded to the
  kernel
- Added new Cmake flag `GKFS_CREATE_CHECK_PARENTS` to controls if the existance of the parent node needs to be checked during
  the creation of a child node.

### Changed

- Daemon logs for RPC handlers have been polished
- Updated Margo, Mercury and Libfabric dependencies

### Fixed

- mk_node RPC wasn't propagating errors correctly from daemons
- README has been improoved and got some minor fixes
- fix wrong path in log call for mk_symlink function

## [0.6.1] - 2019-09-17

### Added

- Added new Cmake flag `LOG_SYSCALLS` to enable/disable syscall logging.
- Intercept the 64 bit version of `getdents`.
- Added debian-based docker image.

### Changed

- Disable syscalls logging by default
- Update Mercury, RocksDB and Libfabric dependencies

### Fixed

- Fix read at the end of file.
- Don't create log file when using `--version`/`--help` cli flags.
- On some systems LD_PRELOAD used on /bin/bash binary was not working.
- Missing definition of `loff_t` on new version of GCC.

## [0.6.0] - 2019-07-26

### Added

- Add compile time option to disable shared memory communication `-DUSE_SHM:BOOL=OFF`

### Changed

- Deamons does not store anymore information about the others deamons.
- Improoved error handling on deamon initialization
- Decreased RPC timeout 3min -> 3sec
- Update 3rd party dependencies

### Removed

- PID file is not used anymore, we use only the new `hosts file` for out of bound communication
- Dropped CCI plugin support
- Dropped hostname-suffix cli option
- Dropped port cli option (use `--listen` instead)
- It is not needed anymore to pass hosts information to deamons, thus the `--hosts` cli have been removed

### Fixed

- Errors on get_dirents RPC are now reported back to clients
- Write errors happenig on deamons are now reported back to clients
- number overflow on lseek didn't allow to use seek on huge files

## [0.5.0] - 2019-04-29

### Changed

- Intercept I/O syscalls instead of GlibC function
  using [syscall intercept library](https://github.com/pmem/syscall_intercept)

## [0.4.0] - 2019-04-18

First GekkoFS public release

This version provides a client library that uses GLibC I/O function interception.

## [0.3.1] - 2018-03-04

### Changed

- Read-write process improved. @Marc vef
- Improved Filemap. @Marc Vef
