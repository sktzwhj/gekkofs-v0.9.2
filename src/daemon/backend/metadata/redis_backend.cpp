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
#include <daemon/backend/exceptions.hpp>
#include <daemon/backend/metadata/metadata_module.hpp>

#include <common/metadata.hpp>
#include <common/path_util.hpp>
#include <mutex>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <daemon/backend/metadata/redis_backend.hpp>
using namespace std;
extern "C" {
#include <sys/stat.h>
}
std::recursive_mutex redis_mutex_;
namespace gkfs::metadata {

/**
 * Find available port for redis server.
 * @param startport 
 */
int find_port(int startport = 6000) {
    int sockfd;
    struct sockaddr_in addr;
    
    for (int port = startport; port < 65535; ++port) {
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            throw std::runtime_error("Failed to create socket.");
        }
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);
        if (bind(sockfd, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
            close(sockfd);
            return port;
        }
        close(sockfd);
    }
    throw std::runtime_error("Failed to find available port for redis server.");
}

/**
 * Called when the daemon is started: Connects to the KV store
 * @param path where KV store data is stored
 */
RedisBackend::RedisBackend(const std::string& path, const std::string& redis_server) {
    int port = find_port();
    std::string redis_bind = " --bind localhost --port " + std::to_string(port);
    std::string redis_dir = " --dir " + path;
    std::string redis_server_command =  redis_server + redis_bind + redis_dir + " &";
    system(redis_server_command.c_str());

    sw::redis::ConnectionOptions connection_options;
    connection_options.host = "localhost";  // Required.
    connection_options.port = port; // Optional. The default port is 6379.
    connection_options.socket_timeout = std::chrono::milliseconds(200);
    auto rds = std::make_unique<sw::redis::Redis>(connection_options);
    if (!rds) {
        throw std::runtime_error("Redis connection failed.");
    }
    this->rds_db_.reset(rds.release());
}


RedisBackend::~RedisBackend() {
    try{
        rds_db_->command("shutdown");
    } catch(const std::exception& e) {
        ;
    }
    this->rds_db_.reset();
}

/**
 * Exception wrapper on Status object. Throws NotFoundException if
 * s.IsNotFound(), general DBException otherwise
 * @param Redis status
 * @throws DBException
 */
void
RedisBackend::throw_status_excpt(const std::string& s) {
    if(s == "Not Found") {
        throw NotFoundException(s);
    } else {
        throw DBException(s);
    }
}


/**
 * Gets a KV store value for a key
 * @param key
 * @return value
 * @throws DBException on failure, NotFoundException if entry doesn't exist
 */
std::string
RedisBackend::get_impl(const std::string& key) const {
    auto val = rds_db_->get(key);
    if(!val) {
        throw_status_excpt("Not Found");
    }
    return *val;
}

/**
 * Puts an entry into the KV store
 * @param key
 * @param val
 * @throws DBException on failure
 */
void
RedisBackend::put_impl(const std::string& key, const std::string& val) {
    auto s = rds_db_->set(key,val);
    if(!s) {
        throw_status_excpt("Put failed.");
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
RedisBackend::put_no_exist_impl(const std::string& key,
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
RedisBackend::remove_impl(const std::string& key) {
    auto s = rds_db_->del(key);
    if(!s) {
        throw_status_excpt("Not Found");
    }
}

/**
 * checks for existence of an entry
 * @param key
 * @return true if exists
 * @throws DBException on failure
 */
bool
RedisBackend::exists_impl(const std::string& key) {
    auto s = rds_db_->exists(key);
    if(!s) {
        return false;
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
RedisBackend::update_impl(const std::string& old_key,
                            const std::string& new_key,
                            const std::string& val) {
    // TODO use rdb::Put() method
    if(new_key != old_key) {
        remove(old_key);
    }
    put(new_key,val);
}

/**
 * Updates the size on the metadata
 * Operation. E.g., called before a write() call
 *
 * @param key
 * @param io_size
 * @param offset
 * @param append
 * @return offset where the write operation should start. This is only used when
 * append is set
 */
off_t
RedisBackend::increase_size_impl(const std::string& key, size_t io_size,
                                   off_t offset, bool append) {
    lock_guard<recursive_mutex> lock_guard(redis_mutex_);
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
RedisBackend::decrease_size_impl(const std::string& key, size_t size) {
    lock_guard<recursive_mutex> lock_guard(redis_mutex_);
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
RedisBackend::get_dirents_impl(const std::string& dir) const {
    auto root_path = dir;
    long long cursor = 0;
    unsigned int count = 50;
    std::vector<std::string> keys;
    std::vector<std::pair<std::string, bool>> entries;
    do {
        cursor = rds_db_->scan(cursor, root_path + "*", count, std::inserter(keys, keys.begin()));
    } while (cursor); 
    for (auto key : keys){
        if(key.size() == root_path.size()) 
            continue; 
        if(key.find_first_of('/', root_path.size()) != std::string::npos) 
            continue; 
        auto name = key.substr(root_path.size());
        assert(!name.empty());
        Metadata md(get(key));
#ifdef HAS_RENAME
        // Remove entries with negative blocks (rename)
        if(md.blocks() == -1) 
            continue;
#endif // HAS_RENAME
        auto is_dir = S_ISDIR(md.mode());
        entries.emplace_back(std::move(name), is_dir);
    }
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
RedisBackend::get_dirents_extended_impl(const std::string& dir) const {
    auto root_path = dir;
    long long cursor = 0;
    unsigned int count = 50;
    std::vector<std::string> keys;
    std::vector<std::tuple<std::string, bool, size_t, time_t>> entries;
    do {
        cursor = rds_db_->scan(cursor, root_path + "*", count, std::inserter(keys, keys.begin()));
    } while (cursor); 
    for (auto key : keys){
        if(key.size() == root_path.size()) 
            continue; 
        if(key.find_first_of('/', root_path.size()) != std::string::npos) 
            continue; 
        auto name = key.substr(root_path.size());
        assert(!name.empty());
        Metadata md(get(key));
#ifdef HAS_RENAME
        // Remove entries with negative blocks (rename)
        if(md.blocks() == -1) 
            continue;
#endif // HAS_RENAME
        auto is_dir = S_ISDIR(md.mode());
        entries.emplace_back(std::forward_as_tuple(std::move(name), is_dir,
                                                   md.size(), md.ctime()));
    }
    return entries;
}


/**
 * Code example for iterating all entries in KV store. This is for debug only as
 * it is too expensive
 */
void
RedisBackend::iterate_all_impl() const {
    ;
}

/**
 * Used for setting KV store settings
 */
void
RedisBackend::optimize_database_impl() {
    rds_db_->command("CONFIG", "SET", "maxmemory-policy", "noeviction");
}

} // namespace gkfs::metadata
