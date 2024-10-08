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
#include <daemon/daemon.hpp>
#include <daemon/backend/metadata/db.hpp>
#include <daemon/backend/exceptions.hpp>
#include <daemon/backend/metadata/metadata_module.hpp>

#include <common/metadata.hpp>
#include <common/path_util.hpp>
#include <iostream>
#include <daemon/backend/metadata/parallax_backend.hpp>
#include <mutex>
#include <unistd.h>
#include <fcntl.h>
using namespace std;
extern "C" {
#include <sys/stat.h>
}

std::recursive_mutex parallax_mutex_;

namespace gkfs::metadata {
/**
 * @brief Destroy the Kreon Backend:: Kreon Backend object
 * We remove the file, too large for the CI.
 * TODO: Insert option
 */
ParallaxBackend::~ParallaxBackend() {
    auto ret = par_close(par_db_);
    if(ret) {
        std::runtime_error(fmt::format("Failed to close parallax {}", ret));
    }
}

/**
 * Called when the daemon is started: Connects to the KV store
 * @param path where KV store data is stored
 */
ParallaxBackend::ParallaxBackend(const std::string& path)
    : par_path_(std::move(path)) {

    // We try to open options.yml if it exists, if not we create it by default
    int options = open("options.yml", O_RDWR | O_CREAT, 0644);
    int64_t sizeOptions;
    sizeOptions = lseek(options, 0, SEEK_END);
    if(sizeOptions == 0) {
        std::string optcontent =
                "level0_size: 64\ngc_interval: 10\ngrowth_factor: 4\nmedium_log_LRU_cache_size: 400\nlevel_medium_inplace: 3\n";
        auto write_size =
                write(options, optcontent.c_str(), optcontent.length());
        if(write_size < 0 ||
           static_cast<unsigned long>(write_size) < optcontent.length()) {
            throw std::runtime_error(fmt::format(
                    "Failed to write Parallax options.yml: err '{}'",
                    write_size));
        }
    }

    close(options);
    int64_t size;

    int fd = open(par_path_.c_str(), O_RDWR | O_CREAT, 0644);
    if(fd < 0) {
        throw std::runtime_error(
                fmt::format("Failed to open Parallax DB file. fd '{}'", fd));
    }

    // Check size if we want to reuse it
    size = lseek(fd, 0, SEEK_END);
    if(size == -1) {
        throw std::runtime_error(fmt::format(
                "[{}:{}:{}] failed to determine volume size exiting...",
                __FILE__, __func__, __LINE__));
    }

    if(size == 0) {
        size = GKFS_DATA->parallax_size_md();

        lseek(fd, size - 1, SEEK_SET);
        std::string tmp = "x";
        auto write_size = write(fd, tmp.c_str(), 1);
        if(write_size < 1) {
            throw std::runtime_error(
                    fmt::format("Failed to write to Parallax db file: err '{}'",
                                write_size));
        }
        close(fd);

        // We format the database TODO this doesn't work kv_format.parallax is
        // not in path
        std::string cmd = "kv_format.parallax --device " + par_path_ +
                          " --max_regions_num 1 ";
        auto err = system(cmd.c_str());
        if(err < 0) {
            throw std::runtime_error(fmt::format(
                    "Failed to format parallax device: err '{}'", err));
        }
    }

    par_options_.create_flag = PAR_CREATE_DB;
    par_options_.db_name = "test";
    par_options_.volume_name = (char*) malloc(par_path_.size() + 1);
    strcpy(par_options_.volume_name, par_path_.c_str());
    const char* error = NULL;
    par_options_.options = par_get_default_options();
    par_db_ = par_open(&par_options_, &error);
    if(par_db_ == nullptr) {
        throw std::runtime_error(
                fmt::format("Failed to open database: err {}", *error));
    }
}


/**
 * Exception wrapper on Status object. Throws NotFoundException if
 * s == "Not Found", general DBException otherwise
 * @param String with status
 * @throws DBException
 */
void
ParallaxBackend::throw_status_excpt(const std::string& s) {
    if(s == "Not Found") {
        throw NotFoundException(s);
    } else {
        throw DBException(s);
    }
}

/**
 * Convert a String to klc_key
 * @param key
 * @param par_key struct
 */
inline void
ParallaxBackend::str2par(const std::string& key, struct par_key& K) const {
    K.size = key.size();
    K.data = key.c_str();
}

/**
 * Convert a String to klc_value
 * @param value
 * @param par_value struct
 */
inline void
ParallaxBackend::str2par(const std::string& value, struct par_value& V) const {
    V.val_size = value.size() + 1;
    V.val_buffer = (char*) value.c_str();
}

/**
 * Gets a KV store value for a key
 * @param key
 * @return value
 * @throws DBException on failure, NotFoundException if entry doesn't exist
 */
std::string
ParallaxBackend::get_impl(const std::string& key) const {
    std::string val;

    struct par_key K;
    struct par_value V;
    V.val_buffer = NULL;
    str2par(key, K);
    const char* error = NULL;
    par_get(par_db_, &K, &V, &error);
    if(V.val_buffer == NULL) {
        throw_status_excpt("Not Found");
    } else {
        val = V.val_buffer;
        free(V.val_buffer);
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
ParallaxBackend::put_impl(const std::string& key, const std::string& val) {

    struct par_key_value key_value;

    str2par(key, key_value.k);
    str2par(val, key_value.v);
    const char* error = NULL;
    par_put(par_db_, &key_value, &error);
    if(error) {
        throw_status_excpt(fmt::format("Failed to put_impl: err {}", *error));
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
ParallaxBackend::put_no_exist_impl(const std::string& key,
                                   const std::string& val) {

    struct par_key_value key_value;
    str2par(key, key_value.k);
    str2par(val, key_value.v);

    par_ret_code ret = par_exists(par_db_, &key_value.k);
    if(ret == PAR_KEY_NOT_FOUND) {
        const char* error = NULL;
        par_put(par_db_, &key_value, &error);
        if(error) {
            throw_status_excpt(
                    fmt::format("Failed to put_no_exist_impl: err {}", *error));
        }
    } else
        throw ExistsException(key);
}

/**
 * Removes an entry from the KV store
 * @param key
 * @throws DBException on failure, NotFoundException if entry doesn't exist
 */
void
ParallaxBackend::remove_impl(const std::string& key) {

    struct par_key k;

    str2par(key, k);
    const char* error = NULL;
    par_delete(par_db_, &k, &error);

    if(error) {
        throw_status_excpt(
                fmt::format("Failed to remove_impl: err {}", *error));
    }
}

/**
 * checks for existence of an entry
 * @param key
 * @return true if exists
 * @throws DBException on failure
 */
bool
ParallaxBackend::exists_impl(const std::string& key) {

    struct par_key k;

    str2par(key, k);

    par_ret_code ret = par_exists(par_db_, &k);
    if(ret == PAR_KEY_NOT_FOUND) {
        return true;
    }

    return false; // TODO it is not the only case, we can have errors
}

/**
 * Updates a metadentry atomically and also allows to change keys
 * @param old_key
 * @param new_key
 * @param val
 * @throws DBException on failure, NotFoundException if entry doesn't exist
 */
void
ParallaxBackend::update_impl(const std::string& old_key,
                             const std::string& new_key,
                             const std::string& val) {

    // TODO: Check Parallax transactions/Batches

    struct par_key_value n_key_value;
    struct par_key o_key;

    str2par(new_key, n_key_value.k);
    str2par(val, n_key_value.v);
    str2par(old_key, o_key);

    const char* error = NULL;
    if(new_key != old_key) {
        par_delete(par_db_, &o_key, &error);
        if(error) {
            throw_status_excpt(
                    fmt::format("Failed to delete (update): err {}", *error));
        }
    }

    par_put(par_db_, &n_key_value, &error);
    if(error) {
        throw_status_excpt(
                fmt::format("Failed to update/put_impl: err {}", *error));
    }
}

/**
 * Updates the size on the metadata
 * Operation. E.g., called before a write() call
 * @param key
 * @param size
 * @param append
 * @throws DBException on failure
 */
off_t
ParallaxBackend::increase_size_impl(const std::string& key, size_t io_size,
                                    off_t offset, bool append) {
    lock_guard<recursive_mutex> lock_guard(parallax_mutex_);
    off_t out_offset = -1;
    auto value = get(key);
    // Decompress string
    Metadata md(value);
    if(append) {
        out_offset = md.size();
        md.size(md.size() + io_size);
    } else
        md.size(offset + io_size);
    update(key, key, md.serialize());
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
ParallaxBackend::decrease_size_impl(const std::string& key, size_t size) {
    lock_guard<recursive_mutex> lock_guard(parallax_mutex_);

    auto value = get(key);
    // Decompress string
    Metadata md(value);
    md.size(size);
    update(key, key, md.serialize());
}

/**
 * Return all the first-level entries of the directory @dir
 *
 * @return vector of pair <std::string name, bool is_dir>,
 *         where name is the name of the entries and is_dir
 *         is true in the case the entry is a directory.
 */
std::vector<std::pair<std::string, bool>>
ParallaxBackend::get_dirents_impl(const std::string& dir) const {
    auto root_path = dir;
    struct par_key K;

    str2par(root_path, K);
    const char* error = NULL;
    par_scanner S = par_init_scanner(par_db_, &K, PAR_GREATER_OR_EQUAL, &error);
    if(error) {
        throw_status_excpt(
                fmt::format("Failed get_dirents_imp: err {}", *error));
    }
    std::vector<std::pair<std::string, bool>> entries;

    while(par_is_valid(S)) {
        struct par_key K2 = par_get_key(S);
        struct par_value value = par_get_value(S);

        std::string k(K2.data, K2.size);
        std::string v(value.val_buffer, value.val_size);
        if(k.size() < root_path.size() ||
           k.substr(0, root_path.size()) != root_path) {
            break;
        }
+
        if(k.size() == root_path.size()) {
            par_get_next(S);
            continue;
        }

        /***** Get File name *****/
        auto name = k;
        if(name.find_first_of('/', root_path.size()) != std::string::npos) {
            // skip stuff deeper then one level depth
            par_get_next(S);
            continue;
        }

        // remove prefix
        name = name.substr(root_path.size());

        // relative path of directory entries must not be empty
        assert(!name.empty());

        Metadata md(v);
#ifdef HAS_RENAME
        // Remove entries with negative blocks (rename)
        if(md.blocks() == -1) {
            if(par_get_next(S) && !par_is_valid(S))
                break;
            else
                continue;
        }
#endif // HAS_RENAME
        auto is_dir = S_ISDIR(md.mode());

        entries.emplace_back(std::move(name), is_dir);

        par_get_next(S);
    }
    // If we don't close the scanner we cannot delete keys
    par_close_scanner(S);

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
ParallaxBackend::get_dirents_extended_impl(const std::string& dir) const {
    auto root_path = dir;
    //   assert(gkfs::path::is_absolute(root_path));
    // add trailing slash if missing
    if(!gkfs::path::has_trailing_slash(root_path) && root_path.size() != 1) {
        // add trailing slash only if missing and is not the root_folder "/"
        root_path.push_back('/');
    }

    struct par_key K;

    str2par(root_path, K);
    const char* error = NULL;
    par_scanner S = par_init_scanner(par_db_, &K, PAR_GREATER_OR_EQUAL, &error);
    if(error) {
        throw_status_excpt(fmt::format(
                "Failed to get_dirents_extended_impl: err {}", *error));
    }

    std::vector<std::tuple<std::string, bool, size_t, time_t>> entries;

    while(par_is_valid(S)) {
        struct par_key K2 = par_get_key(S);
        struct par_value value = par_get_value(S);

        std::string k(K2.data, K2.size);
        std::string v(value.val_buffer, value.val_size);

        if(k.size() < root_path.size() ||
           k.substr(0, root_path.size()) != root_path) {
            break;
        }

        if(k.size() == root_path.size()) {
            if(par_get_next(S) && !par_is_valid(S))
                break;
            continue;
        }

        /***** Get File name *****/
        auto name = k;
        if(name.find_first_of('/', root_path.size()) != std::string::npos) {
            // skip stuff deeper then one level depth
            if(par_get_next(S) && !par_is_valid(S))
                break;
            continue;
        }
        // remove prefix
        name = name.substr(root_path.size());

        // relative path of directory entries must not be empty
        assert(!name.empty());

        Metadata md(v);
#ifdef HAS_RENAME
        // Remove entries with negative blocks (rename)
        if(md.blocks() == -1) {
            if(par_get_next(S) && !par_is_valid(S))
                break;
            else
                continue;
        }
#endif // HAS_RENAME
        auto is_dir = S_ISDIR(md.mode());

        entries.emplace_back(std::forward_as_tuple(std::move(name), is_dir,
                                                   md.size(), md.ctime()));

        if(par_get_next(S) && !par_is_valid(S))
            break;
    }
    // If we don't close the scanner we cannot delete keys
    par_close_scanner(S);

    return entries;
}


/**
 * Code example for iterating all entries in KV store. This is for debug only as
 * it is too expensive
 */
void
ParallaxBackend::iterate_all_impl() const {}


} // namespace gkfs::metadata
