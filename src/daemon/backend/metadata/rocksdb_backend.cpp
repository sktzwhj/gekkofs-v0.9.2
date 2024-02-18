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
#include <iostream>
#include <daemon/backend/metadata/rocksdb_backend.hpp>
extern "C" {
#include <sys/stat.h>
}

namespace gkfs::metadata {

/**
 * Called when the daemon is started: Connects to the KV store
 * @param path where KV store data is stored
 */
RocksDBBackend::RocksDBBackend(const std::string& path) {

    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options_.IncreaseParallelism();
    options_.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options_.create_if_missing = true;
    options_.merge_operator.reset(new MetadataMergeOperator);
    optimize_database_impl();
    write_opts_.disableWAL = !(gkfs::config::rocksdb::use_write_ahead_log);
    rdb::DB* rdb_ptr = nullptr;
    auto s = rocksdb::DB::Open(options_, path, &rdb_ptr);
    if(!s.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " + s.ToString());
    }
    this->db_.reset(rdb_ptr);
}


RocksDBBackend::~RocksDBBackend() {
    this->db_.reset();
}

/**
 * Exception wrapper on Status object. Throws NotFoundException if
 * s.IsNotFound(), general DBException otherwise
 * @param RocksDB status
 * @throws DBException
 */
void
RocksDBBackend::throw_status_excpt(const rdb::Status& s) {
    assert(!s.ok());

    if(s.IsNotFound()) {
        throw NotFoundException(s.ToString());
    } else {
        throw DBException(s.ToString());
    }
}


/**
 * Gets a KV store value for a key
 * @param key
 * @return value
 * @throws DBException on failure, NotFoundException if entry doesn't exist
 */
std::string
RocksDBBackend::get_impl(const std::string& key) const {
    std::string val;

    auto s = db_->Get(rdb::ReadOptions(), key, &val);
    if(!s.ok()) {
        throw_status_excpt(s);
    }

    return val;
}

/**
 * Puts an entry into the KV store
 * @param key
 * @param val
 * @throws DBException on failure
 */
void
RocksDBBackend::put_impl(const std::string& key, const std::string& val) {

    auto cop = CreateOperand(val);
    auto s = db_->Merge(write_opts_, key, cop.serialize());
    if(!s.ok()) {
        throw_status_excpt(s);
    }
}

/**
 * Puts an entry into the KV store if it doesn't exist. This function does not
 * use a mutex.
 * @param key
 * @param val
 * @throws DBException on failure, ExistException if entry already exists
 */
void
RocksDBBackend::put_no_exist_impl(const std::string& key,
                                  const std::string& val) {

    if(exists(key))
        throw ExistsException(key);
    put(key, val);
}

/**
 * Removes an entry from the KV store
 * @param key
 * @throws DBException on failure, NotFoundException if entry doesn't exist
 */
void
RocksDBBackend::remove_impl(const std::string& key) {

    auto s = db_->Delete(write_opts_, key);
    if(!s.ok()) {
        throw_status_excpt(s);
    }
}

/**
 * checks for existence of an entry
 * @param key
 * @return true if exists
 * @throws DBException on failure
 */
bool
RocksDBBackend::exists_impl(const std::string& key) {

    std::string val;

    auto s = db_->Get(rdb::ReadOptions(), key, &val);
    if(!s.ok()) {
        if(s.IsNotFound()) {
            return false;
        } else {
            throw_status_excpt(s);
        }
    }
    return true;
}

/**
 * Updates a metadentry atomically and also allows to change keys
 * @param old_key
 * @param new_key
 * @param val
 * @throws DBException on failure, NotFoundException if entry doesn't exist
 */
void
RocksDBBackend::update_impl(const std::string& old_key,
                            const std::string& new_key,
                            const std::string& val) {

    // TODO use rdb::Put() method
    rdb::WriteBatch batch;
    batch.Delete(old_key);
    batch.Put(new_key, val);
    auto s = db_->Write(write_opts_, &batch);
    if(!s.ok()) {
        throw_status_excpt(s);
    }
}

/**
 * Updates the size on the metadata
 * Operation. E.g., called before a write() call
 *
 * A special case represents the append operation. Since multiple processes
 * could want to append a file in parallel, the corresponding offsets where the
 * write operation starts, needs to be reserved. This is an expensive operation
 * as we need to force a RocksDB Merge operation to receive the starting offset
 * for this write request.
 *
 * @param key
 * @param io_size
 * @param offset
 * @param append
 * @return offset where the write operation should start. This is only used when
 * append is set
 */
off_t
RocksDBBackend::increase_size_impl(const std::string& key, size_t io_size,
                                   off_t offset, bool append) {
    off_t out_offset = -1;
    if(append) {
        auto merge_id = gkfs::metadata::gen_unique_id(key);
        // no offset needed because new size is current file size + io_size
        auto uop = IncreaseSizeOperand(io_size, merge_id, append);
        auto s = db_->Merge(write_opts_, key, uop.serialize());
        if(!s.ok()) {
            throw_status_excpt(s);
        } else {
            // force merge operation to run
            get_impl(key);
            try {
                // the offset was added during FullMergeV2() call
                out_offset =
                        GKFS_METADATA_MOD->append_offset_reserve_get_and_erase(
                                merge_id);
            } catch(std::out_of_range& e) {
                GKFS_METADATA_MOD->log()->warn(
                        "{}() - out_of_range exception: {} when attempting to get offset for key {}",
                        __func__, e.what(), key);
            }
        }
    } else {
        // In the standard case we simply add the I/O request size to the
        // offset.
        auto uop = IncreaseSizeOperand(offset + io_size);
        auto s = db_->Merge(write_opts_, key, uop.serialize());
        if(!s.ok()) {
            throw_status_excpt(s);
        }
    }
    return out_offset;
}

/**
 * Decreases the size on the metadata
 * Operation E.g., called before a truncate() call
 * @param key
 * @param size
 * @throws DBException on failure
 */
void
RocksDBBackend::decrease_size_impl(const std::string& key, size_t size) {

    auto uop = DecreaseSizeOperand(size);
    auto s = db_->Merge(write_opts_, key, uop.serialize());
    if(!s.ok()) {
        throw_status_excpt(s);
    }
}

/**
 * Return all the first-level entries of the directory @dir
 *
 * @return vector of pair <std::string name, bool is_dir>,
 *         where name is the name of the entries and is_dir
 *         is true in the case the entry is a directory.
 */
std::vector<std::pair<std::string, bool>>
RocksDBBackend::get_dirents_impl(const std::string& dir) const {
    auto root_path = dir;
    rocksdb::ReadOptions ropts;
    auto it = db_->NewIterator(ropts);

    std::vector<std::pair<std::string, bool>> entries;
    for(it->Seek(root_path); it->Valid() && it->key().starts_with(root_path);
        it->Next()) {

        if(it->key().size() == root_path.size()) {
            // we skip this path cause it is exactly the root_path
            continue;
        }

        /***** Get File name *****/
        auto name = it->key().ToString();
        if(name.find_first_of('/', root_path.size()) != std::string::npos) {
            // skip stuff deeper then one level depth
            continue;
        }
        // remove prefix
        name = name.substr(root_path.size());

        // relative path of directory entries must not be empty
        assert(!name.empty());

        Metadata md(it->value().ToString());
#ifdef HAS_RENAME
        // Remove entries with negative blocks (rename)
        if(md.blocks() == -1) {
            continue;
        }
#endif // HAS_RENAME
        auto is_dir = S_ISDIR(md.mode());

        entries.emplace_back(std::move(name), is_dir);
    }
    assert(it->status().ok());
    return entries;
}

/**
 * Return all the first-level entries of the directory @dir
 *
 * @return vector of pair <std::string name, bool is_dir - size - ctime>,
 *         where name is the name of the entries and is_dir
 *         is true in the case the entry is a directory.
 */
std::vector<std::tuple<std::string, bool, size_t, time_t>>
RocksDBBackend::get_dirents_extended_impl(const std::string& dir) const {
    auto root_path = dir;
    rocksdb::ReadOptions ropts;
    auto it = db_->NewIterator(ropts);

    std::vector<std::tuple<std::string, bool, size_t, time_t>> entries;

    for(it->Seek(root_path); it->Valid() && it->key().starts_with(root_path);
        it->Next()) {

        if(it->key().size() == root_path.size()) {
            // we skip this path cause it is exactly the root_path
            continue;
        }

        /***** Get File name *****/
        auto name = it->key().ToString();
        if(name.find_first_of('/', root_path.size()) != std::string::npos) {
            // skip stuff deeper then one level depth
            continue;
        }
        // remove prefix
        name = name.substr(root_path.size());

        // relative path of directory entries must not be empty
        assert(!name.empty());

        Metadata md(it->value().ToString());
#ifdef HAS_RENAME
        // Remove entries with negative blocks (rename)
        if(md.blocks() == -1) {
            continue;
        }
#endif // HAS_RENAME
        auto is_dir = S_ISDIR(md.mode());

        entries.emplace_back(std::forward_as_tuple(std::move(name), is_dir,
                                                   md.size(), md.ctime()));
    }
    assert(it->status().ok());
    return entries;
}


/**
 * Code example for iterating all entries in KV store. This is for debug only as
 * it is too expensive
 */
void
RocksDBBackend::iterate_all_impl() const {
    std::string key;
    std::string val;
    // Do RangeScan on parent inode
    auto iter = db_->NewIterator(rdb::ReadOptions());
    for(iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        key = iter->key().ToString();
        val = iter->value().ToString();
        std::cout << key << std::endl;
    }
}

/**
 * Used for setting KV store settings
 */
void
RocksDBBackend::optimize_database_impl() {
    options_.max_successive_merges = 128;
}


} // namespace gkfs::metadata
