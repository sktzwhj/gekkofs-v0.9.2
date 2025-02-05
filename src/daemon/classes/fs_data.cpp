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

#include <daemon/classes/fs_data.hpp>
#include <daemon/backend/metadata/db.hpp>

#include <spdlog/spdlog.h>

namespace gkfs::daemon {

// getter/setter

const std::shared_ptr<spdlog::logger>&
FsData::spdlogger() const {
    return spdlogger_;
}

void
FsData::spdlogger(const std::shared_ptr<spdlog::logger>& spdlogger) {
    FsData::spdlogger_ = spdlogger;
}

const std::shared_ptr<gkfs::metadata::MetadataDB>&
FsData::mdb() const {
    return mdb_;
}

void
FsData::mdb(const std::shared_ptr<gkfs::metadata::MetadataDB>& mdb) {
    mdb_ = mdb;
}

void
FsData::close_mdb() {
    mdb_.reset();
}

const std::shared_ptr<gkfs::data::ChunkStorage>&
FsData::storage() const {
    return storage_;
}

void
FsData::storage(const std::shared_ptr<gkfs::data::ChunkStorage>& storage) {
    storage_ = storage;
}

const std::string&
FsData::rootdir() const {
    return rootdir_;
}

void
FsData::rootdir(const std::string& rootdir) {
    FsData::rootdir_ = rootdir;
}

const std::string&
FsData::rootdir_suffix() const {
    return rootdir_suffix_;
}

void
FsData::rootdir_suffix(const std::string& rootdir_suffix) {
    FsData::rootdir_suffix_ = rootdir_suffix;
}

const std::string&
FsData::mountdir() const {
    return mountdir_;
}

void
FsData::mountdir(const std::string& mountdir) {
    FsData::mountdir_ = mountdir;
}

const std::string&
FsData::metadir() const {
    return metadir_;
}

void
FsData::metadir(const std::string& metadir) {
    FsData::metadir_ = metadir;
}

std::string_view
FsData::dbbackend() const {
    return dbbackend_;
}

void
FsData::dbbackend(const std::string& dbbackend) {
    FsData::dbbackend_ = dbbackend;
}

const std::string&
FsData::server() const {
    return server_;
}

void
FsData::server(const std::string& server) {
    FsData::server_ = server;
}

const std::string&
FsData::rpc_protocol() const {
    return rpc_protocol_;
}

void
FsData::rpc_protocol(const std::string& rpc_protocol) {
    rpc_protocol_ = rpc_protocol;
}

const std::string&
FsData::bind_addr() const {
    return bind_addr_;
}

void
FsData::bind_addr(const std::string& addr) {
    bind_addr_ = addr;
}

const std::string&
FsData::hosts_file() const {
    return hosts_file_;
}

void
FsData::hosts_file(const std::string& lookup_file) {
    hosts_file_ = lookup_file;
}

bool
FsData::use_auto_sm() const {
    return use_auto_sm_;
}

void
FsData::use_auto_sm(bool use_auto_sm) {
    use_auto_sm_ = use_auto_sm;
}

bool
FsData::atime_state() const {
    return atime_state_;
}

void
FsData::atime_state(bool atime_state) {
    FsData::atime_state_ = atime_state;
}

bool
FsData::mtime_state() const {
    return mtime_state_;
}

void
FsData::mtime_state(bool mtime_state) {
    FsData::mtime_state_ = mtime_state;
}

bool
FsData::ctime_state() const {
    return ctime_state_;
}

void
FsData::ctime_state(bool ctime_state) {
    FsData::ctime_state_ = ctime_state;
}

bool
FsData::link_cnt_state() const {
    return link_cnt_state_;
}

void
FsData::link_cnt_state(bool link_cnt_state) {
    FsData::link_cnt_state_ = link_cnt_state;
}

bool
FsData::blocks_state() const {
    return blocks_state_;
}

void
FsData::blocks_state(bool blocks_state) {
    FsData::blocks_state_ = blocks_state;
}

unsigned long long
FsData::parallax_size_md() const {
    return parallax_size_md_;
}

void
FsData::parallax_size_md(unsigned int size_md) {
    FsData::parallax_size_md_ = static_cast<unsigned long long>(
            size_md * 1024ull * 1024ull * 1024ull);
}

const std::shared_ptr<gkfs::utils::Stats>&
FsData::stats() const {
    return stats_;
}

void
FsData::stats(const std::shared_ptr<gkfs::utils::Stats>& stats) {
    FsData::stats_ = stats;
}

void
FsData::close_stats() {
    stats_.reset();
}

bool
FsData::enable_stats() const {
    return enable_stats_;
}

void
FsData::enable_stats(bool enable_stats) {
    FsData::enable_stats_ = enable_stats;
}

bool
FsData::enable_chunkstats() const {
    return enable_chunkstats_;
}

void
FsData::enable_chunkstats(bool enable_chunkstats) {
    FsData::enable_chunkstats_ = enable_chunkstats;
}

bool
FsData::enable_prometheus() const {
    return enable_prometheus_;
}

void
FsData::enable_prometheus(bool enable_prometheus) {
    FsData::enable_prometheus_ = enable_prometheus;
}

const std::string&
FsData::stats_file() const {
    return stats_file_;
}

void
FsData::stats_file(const std::string& stats_file) {
    FsData::stats_file_ = stats_file;
}

const std::string&
FsData::prometheus_gateway() const {
    return prometheus_gateway_;
}

void
FsData::prometheus_gateway(const std::string& prometheus_gateway) {
    FsData::prometheus_gateway_ = prometheus_gateway;
}

} // namespace gkfs::daemon
