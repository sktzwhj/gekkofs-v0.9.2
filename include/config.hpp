/*
  Copyright 2018-2024, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2024, Johannes Gutenberg Universitaet Mainz, Germany

  This software was partially supported by the
  EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).

  This software was partially supported by the
  ADA-FS project under the SPPEXA project funded by the DFG.

  This file is part of GekkoFS.

  GekkoFS is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  GekkoFS is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with GekkoFS.  If not, see <https://www.gnu.org/licenses/>.

  SPDX-License-Identifier: GPL-3.0-or-later
*/

#ifndef GEKKOFS_CONFIG_HPP
#define GEKKOFS_CONFIG_HPP

#include <common/cmake_configure.hpp>
#include <array>
#include <cstdint>
// environment prefixes (are concatenated in env module at compile time)
#define CLIENT_ENV_PREFIX "LIBGKFS_"
#define DAEMON_ENV_PREFIX "GKFS_DAEMON_"
#define COMMON_ENV_PREFIX "GKFS_"
#define KB *1024
#define MB *1024*1024
#define GB *1024*1024*1024
#define TB *1024*1024*1024*1024
namespace gkfs::config {

constexpr auto hostfile_path = "./gkfs_hosts.txt";
constexpr auto hostfile_config_path = "./gkfs_hosts_config.txt";
constexpr auto forwarding_file_path = "./gkfs_forwarding.map";
constexpr auto registryfile_path = "./gkfs_registry.txt";
constexpr auto merge_default = "off";
constexpr auto use_registry = "off";

namespace io {
/*
 * Zero buffer before read. This is relevant if sparse files are used.
 * If buffer is not zeroed, sparse regions contain invalid data.
 */
constexpr auto zero_buffer_before_read = false;
} // namespace io

namespace log {
constexpr auto client_log_path = "/tmp/gkfs_client.log";
constexpr auto daemon_log_path = "/tmp/gkfs_daemon.log";

constexpr auto client_log_level = "info,errors,critical,hermes";
constexpr auto daemon_log_level = 4; // info
} // namespace log

namespace metadata {
// directory name where the metadata db instance is placed
constexpr auto dir = "metadata";

// which metadata should be considered apart from size and mode
// Blocks are used to store the rename status (-1 is a renamed file)
constexpr auto use_atime = false;
constexpr auto use_ctime = false;
constexpr auto use_mtime = false;
constexpr auto use_link_cnt = false;
#ifdef HAS_RENAME
constexpr auto use_blocks = true;
#else
constexpr auto use_blocks = false;
#endif // HAS_RENAME
/*
 * If true, all chunks on the same host are removed during a metadata remove
 * rpc. This is a technical optimization that reduces the number of RPCs for
 * remove operations. This setting could be useful for future asynchronous
 * remove implementations where the data should not be removed immediately.
 */
constexpr auto implicit_data_removal = true;

// metadata logic
// Check for existence of file metadata before create. This done on RocksDB
// level
constexpr auto create_exist_check = true;
} // namespace metadata
namespace data {
// directory name below rootdir where chunks are placed
constexpr auto chunk_dir = "chunks";
} // namespace data

namespace rpc {
constexpr auto chunksize = 524288; // in bytes (e.g., 524288 == 512KB)
/* PFL configuration */
constexpr auto use_PFL = false;
constexpr auto PFLcomponents = 6; //number of regions to seperate
constexpr std::array<uint64_t, PFLcomponents> PFLlayout = {0, 4 MB, 8 MB, 16 MB, 32 MB, 64 MB}; //layout :start offset of each component
constexpr std::array<uint64_t, PFLcomponents> PFLsize = {512 KB, 1 MB, 2 MB, 4 MB, 8 MB, 16 MB};//stripe size
constexpr std::array<uint64_t, PFLcomponents> PFLcount = {1, 3, 8, 16, 32, 64}; //stripe count 
inline const std::array<uint64_t, PFLcomponents >  generateID() {
    std::array<uint64_t, PFLcomponents >  chunkid;
    int pre_id = 0, pre = 0;
    chunkid[0] = 0;
    for(unsigned int i = 1 ; i <PFLlayout.size(); i++){
      chunkid[i] = (PFLlayout[i] - pre)/PFLsize[i-1] + pre_id ;
      pre_id = (PFLlayout[i] - pre)/PFLsize[i-1] + pre_id;
      pre = PFLlayout[i];
    }
    return chunkid;
}
const std::array<uint64_t, PFLcomponents>  PFLchunkID = generateID();   //chunk start id of each component
/* PFL configuration */

// size of preallocated buffer to hold directory entries in rpc call
constexpr auto dirents_buff_size = (8 * 1024 * 1024); // 8 mega
/*
 * Indicates the number of concurrent progress to drive I/O operations of chunk
 * files to and from local file systems The value is directly mapped to created
 * Argobots xstreams, controlled in a single pool with ABT_snoozer scheduler
 */
constexpr auto daemon_io_xstreams = 8;
// Number of threads used for RPC handlers at the daemon
constexpr auto daemon_handler_xstreams = 4;
} // namespace rpc

namespace rocksdb {
// Write-ahead logging of rocksdb
constexpr auto use_write_ahead_log = false;
} // namespace rocksdb

namespace stats {
constexpr auto max_stats = 1000000; ///< How many stats will be stored
constexpr auto prometheus_gateway = "127.0.0.1:9091";
} // namespace stats

} // namespace gkfs::config

#endif // GEKKOFS_CONFIG_HPP
