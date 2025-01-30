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

#ifndef LFS_FS_DATA_H
#define LFS_FS_DATA_H

#include <daemon/daemon.hpp>

#include <unordered_map>
#include <map>
#include <functional> //std::hash
#include <string_view>

/* Forward declarations */
namespace gkfs {
namespace metadata {
class MetadataDB;
}

namespace data {
class ChunkStorage;
}

/* Forward declarations */
namespace utils {
class Stats;
}

namespace daemon {

class FsData {

private:
    FsData() = default;

    // logger
    std::shared_ptr<spdlog::logger> spdlogger_;

    // paths
    std::string rootdir_{};
    std::string rootdir_suffix_{};
    std::string mountdir_{};
    std::string metadir_{};

    // RPC management
    std::string rpc_protocol_{};
    std::string bind_addr_{};
    std::string hosts_file_{};
    bool use_auto_sm_;

    // Database
    std::shared_ptr<gkfs::metadata::MetadataDB> mdb_;
    std::string dbbackend_;
    std::string server_;

    // Parallax
    unsigned long long parallax_size_md_ = 8589934592ull;

    // Storage backend
    std::shared_ptr<gkfs::data::ChunkStorage> storage_;

    // configurable metadata
    bool atime_state_;
    bool mtime_state_;
    bool ctime_state_;
    bool link_cnt_state_;
    bool blocks_state_;

    // Statistics
    std::shared_ptr<gkfs::utils::Stats> stats_;
    bool enable_stats_ = false;
    bool enable_chunkstats_ = false;
    bool enable_prometheus_ = false;
    std::string stats_file_;

    // Prometheus
    std::string prometheus_gateway_ = gkfs::config::stats::prometheus_gateway;

public:
    static FsData*
    getInstance() {
        static FsData instance;
        return &instance;
    }

    FsData(FsData const&) = delete;

    void
    operator=(FsData const&) = delete;

    // getter/setter

    const std::shared_ptr<spdlog::logger>&
    spdlogger() const;

    void
    spdlogger(const std::shared_ptr<spdlog::logger>& spdlogger_);

    const std::string&
    rootdir() const;

    void
    rootdir(const std::string& rootdir_);

    const std::string&
    rootdir_suffix() const;

    void
    rootdir_suffix(const std::string& rootdir_suffix_);

    const std::string&
    mountdir() const;

    void
    mountdir(const std::string& mountdir_);

    const std::string&
    metadir() const;

    void
    metadir(const std::string& metadir_);

    std::string_view
    dbbackend() const;

    void
    dbbackend(const std::string& dbbackend_);

    const std::string&
    server() const;

    void
    server(const std::string& server_);

    const std::shared_ptr<gkfs::metadata::MetadataDB>&
    mdb() const;

    void
    mdb(const std::shared_ptr<gkfs::metadata::MetadataDB>& mdb);

    void
    close_mdb();

    const std::shared_ptr<gkfs::data::ChunkStorage>&
    storage() const;

    void
    storage(const std::shared_ptr<gkfs::data::ChunkStorage>& storage);

    const std::string&
    rpc_protocol() const;

    void
    rpc_protocol(const std::string& rpc_protocol);

    const std::string&
    bind_addr() const;

    void
    bind_addr(const std::string& addr);

    const std::string&
    hosts_file() const;

    bool
    use_auto_sm() const;

    void
    use_auto_sm(bool use_auto_sm);

    void
    hosts_file(const std::string& lookup_file);

    bool
    atime_state() const;

    void
    atime_state(bool atime_state);

    bool
    mtime_state() const;

    void
    mtime_state(bool mtime_state);

    bool
    ctime_state() const;

    void
    ctime_state(bool ctime_state);

    bool
    link_cnt_state() const;

    void
    link_cnt_state(bool link_cnt_state);

    bool
    blocks_state() const;

    void
    blocks_state(bool blocks_state);

    unsigned long long
    parallax_size_md() const;

    void
    parallax_size_md(unsigned int size_md);

    const std::shared_ptr<gkfs::utils::Stats>&
    stats() const;

    void
    stats(const std::shared_ptr<gkfs::utils::Stats>& stats);

    void
    close_stats();

    bool
    enable_stats() const;

    void
    enable_stats(bool enable_stats);

    bool
    enable_chunkstats() const;

    void
    enable_chunkstats(bool enable_chunkstats);

    bool
    enable_prometheus() const;

    void
    enable_prometheus(bool enable_prometheus);

    const std::string&
    stats_file() const;

    void
    stats_file(const std::string& stats_file);

    const std::string&
    prometheus_gateway() const;

    void
    prometheus_gateway(const std::string& prometheus_gateway_);
};


} // namespace daemon
} // namespace gkfs

#endif // LFS_FS_DATA_H
