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

#include <daemon/backend/metadata/db.hpp>
#include <daemon/backend/metadata/merge.hpp>
#include <daemon/backend/exceptions.hpp>
#include <daemon/backend/metadata/metadata_module.hpp>

#include <common/metadata.hpp>
#include <common/path_util.hpp>
#include <filesystem>

extern "C" {
#include <sys/stat.h>
}
namespace fs = std::filesystem;

namespace gkfs::metadata {

/**
 * Factory to create DB instances
 * @param path where KV store data is stored
 * @param id parallax or rocksdb (default) backend
 */
struct MetadataDBFactory {
    static std::unique_ptr<AbstractMetadataBackend>
    create(const std::string& path, const std::string_view id) {

        if(id == gkfs::metadata::parallax_backend) {
#ifdef GKFS_ENABLE_PARALLAX
            auto metadata_path = fmt::format("{}/{}", path,
                                             gkfs::metadata::parallax_backend);
            GKFS_METADATA_MOD->log()->trace("Using Parallax file '{}'",
                                            metadata_path);
            return std::make_unique<ParallaxBackend>(metadata_path);
#endif
        } else if(id == gkfs::metadata::rocksdb_backend) {
#ifdef GKFS_ENABLE_ROCKSDB
            auto metadata_path =
                    fmt::format("{}/{}", path, gkfs::metadata::rocksdb_backend);
            fs::create_directories(metadata_path);
            GKFS_METADATA_MOD->log()->trace("Using RocksDB directory '{}'",
                                            metadata_path);
            return std::make_unique<RocksDBBackend>(metadata_path);
#endif
        }
        GKFS_METADATA_MOD->log()->error("No valid metadata backend selected");
        exit(EXIT_FAILURE);
    }
};

/**
 * @internal
 * Called when RocksDB connection is established.
 * Used for setting KV store settings
 * see here: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
 * @endinternal
 */
MetadataDB::MetadataDB(const std::string& path, const std::string_view database)
    : path_(path) {

    /* Get logger instance and set it for data module and chunk storage */
    GKFS_METADATA_MOD->log(spdlog::get(GKFS_METADATA_MOD->LOGGER_NAME));
    assert(GKFS_METADATA_MOD->log());
    log_ = spdlog::get(GKFS_METADATA_MOD->LOGGER_NAME);
    assert(log_);

    backend_ = MetadataDBFactory::create(path, database);
}

MetadataDB::~MetadataDB() {
    backend_.reset();
}

std::string
MetadataDB::get(const std::string& key) const {
    return backend_->get(key);
}

void
MetadataDB::put(const std::string& key, const std::string& val) {
    assert(gkfs::path::is_absolute(key));
    assert(key == "/" || !gkfs::path::has_trailing_slash(key));

    backend_->put(key, val);
}

/**
 * @internal
 * This function does not use a mutex.
 * @endinternal
 */
void
MetadataDB::put_no_exist(const std::string& key, const std::string& val) {
    backend_->put_no_exist(key, val);
}

void
MetadataDB::remove(const std::string& key) {
    backend_->remove(key);
}

bool
MetadataDB::exists(const std::string& key) {
    return backend_->exists(key);
}

void
MetadataDB::update(const std::string& old_key, const std::string& new_key,
                   const std::string& val) {
    backend_->update(old_key, new_key, val);
}

off_t
MetadataDB::increase_size(const std::string& key, size_t io_size, off_t offset,
                          bool append) {
    return backend_->increase_size(key, io_size, offset, append);
}

void
MetadataDB::decrease_size(const std::string& key, size_t size) {
    backend_->decrease_size(key, size);
}

std::vector<std::pair<std::string, bool>>
MetadataDB::get_dirents(const std::string& dir) const {
    auto root_path = dir;
    assert(gkfs::path::is_absolute(root_path));
    // add trailing slash if missing
    if(!gkfs::path::has_trailing_slash(root_path) && root_path.size() != 1) {
        // add trailing slash only if missing and is not the root_folder "/"
        root_path.push_back('/');
    }

    return backend_->get_dirents(root_path);
}

std::vector<std::tuple<std::string, bool, size_t, time_t>>
MetadataDB::get_dirents_extended(const std::string& dir) const {
    auto root_path = dir;
    assert(gkfs::path::is_absolute(root_path));
    // add trailing slash if missing
    if(!gkfs::path::has_trailing_slash(root_path) && root_path.size() != 1) {
        // add trailing slash only if missing and is not the root_folder "/"
        root_path.push_back('/');
    }

    return backend_->get_dirents_extended(root_path);
}


/**
 * @internal
 * Code example for iterating all entries in KV store. This is for debug only as
 * it is too expensive.
 * @endinternal
 */
void
MetadataDB::iterate_all() const {
    backend_->iterate_all();
}

} // namespace gkfs::metadata
