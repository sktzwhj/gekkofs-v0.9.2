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
#include <cctype>
#include <sstream>
#include <daemon/backend/metadata/memcached_backend.hpp>
using namespace std;
extern "C" {
#include <sys/stat.h>
}
std::recursive_mutex memcached_mutex_;
namespace gkfs::metadata {

/**
 * Find available port for memcached server.
 * @param startport 
 */
static int find_port(int startport = 6000) {
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
    throw std::runtime_error("Failed to find available port for memcached server.");
}

/**
 * Called when the daemon is started: Connects to the KV store
 * @param path where KV store data is stored
 */
MemcachedBackend::MemcachedBackend(const std::string& path, const std::string& memcached_server) {
    int port = find_port();
    unsigned int pool_size = 4096;
    std::string memcached_bind = " -l localhost -p " + std::to_string(port);
    std::string memcached_pid = " -P " + path + "/memcached.pid ";
    std::string memcached_max_conn = " -c " + std::to_string(pool_size);
    std::string other_options = " -A -M -m 16384 -n 32 ";
    std::string memcached_server_command =  
        memcached_server + memcached_bind + memcached_max_conn + other_options + " &";
    system(memcached_server_command.c_str());
    memcached_st *memc = memcached_create(nullptr);
    if (!memc) {
        throw std::runtime_error("Failed to create Memcached object.");
    }
    memcached_return_t rc = memcached_server_add(memc, "localhost", port);
    if (rc != MEMCACHED_SUCCESS) {
        memcached_free(memc);
        throw std::runtime_error("Failed to connect to Memcached server.");
    }
    mmc_pool_= memcached_pool_create(memc , 5 , pool_size);//初始大小，和最大值，连接池会动态扩展
    if (!mmc_pool_) {
        throw std::runtime_error("Failed to create Memcached connection pool.");
    }
}


MemcachedBackend::~MemcachedBackend() {
    try{
        system("killall -r memcached");
        memcached_pool_destroy(mmc_pool_);
    } catch(const std::exception& e) {
        throw std::runtime_error("Failed to shutdown Memcached server.");
    }
}

/**
 * Exception wrapper on Status object. Throws NotFoundException if
 * s.IsNotFound(), general DBException otherwise
 * @param Memcached status
 * @throws DBException
 */
void
MemcachedBackend::throw_status_excpt(const std::string& s) {
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
MemcachedBackend::get_impl(const std::string& key) const {
    size_t len_val;
    uint32_t flags;
    memcached_return_t rc;

    memcached_st *memc = memcached_pool_pop(mmc_pool_ , NULL , &rc); 
    if (rc != MEMCACHED_SUCCESS) {
        throw_status_excpt(memcached_strerror(memc, rc));
    }
    auto value = memcached_get(memc, key.c_str(), key.length(), 
                              &len_val, &flags, &rc);
    memcached_pool_push(mmc_pool_ , memc);  
    if (rc != MEMCACHED_SUCCESS) {
        throw_status_excpt(memcached_strerror(memc, rc));
    }
    std::string val(value, len_val);
    free(value);
    return val;
}

/**
 * Puts an entry into the KV store
 * @param key
 * @param val
 * @throws DBException on failure
 */
void
MemcachedBackend::put_impl(const std::string& key, const std::string& val) {
    memcached_return_t rc;
    memcached_st *memc = memcached_pool_pop(mmc_pool_ , NULL , &rc); 
    if (rc != MEMCACHED_SUCCESS) {
        throw_status_excpt(memcached_strerror(memc, rc));
    }
    rc = memcached_set(memc, key.c_str(), key.length(),
                                         val.c_str(), val.length(), 0, 0);
    memcached_pool_push(mmc_pool_ , memc);  
    if (rc != MEMCACHED_SUCCESS) {
        throw_status_excpt(memcached_strerror(memc, rc));
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
MemcachedBackend::put_no_exist_impl(const std::string& key,
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
MemcachedBackend::remove_impl(const std::string& key) {
    memcached_return_t rc;
    memcached_st *memc = memcached_pool_pop(mmc_pool_ , NULL , &rc); 
    if (rc != MEMCACHED_SUCCESS) {
        throw_status_excpt(memcached_strerror(memc, rc));
    }
    rc = memcached_delete(memc, key.c_str(), 
                                            key.length(), 0);
    memcached_pool_push(mmc_pool_ , memc);  
    if (rc != MEMCACHED_SUCCESS) {
        throw_status_excpt(memcached_strerror(memc, rc));
    }
}

/**
 * checks for existence of an entry
 * @param key
 * @return true if exists
 * @throws DBException on failure
 */
bool
MemcachedBackend::exists_impl(const std::string& key) {
    memcached_return_t rc;
    memcached_st *memc = memcached_pool_pop(mmc_pool_ , NULL , &rc); 
    if (rc != MEMCACHED_SUCCESS) {
        throw_status_excpt(memcached_strerror(memc, rc));
    }
    bool ans = MEMCACHED_SUCCESS == 
                memcached_exist(memc, key.c_str(), key.length()) ;
    memcached_pool_push(mmc_pool_ , memc);  
    return ans;
}

/**
 * Updates a metadentry atomically and also allows to change keys
 * @param old_key
 * @param new_key
 * @param val
 * @throws DBException on failure, NotFoundException if entry doesn't exist
 */
void
MemcachedBackend::update_impl(const std::string& old_key,
                            const std::string& new_key,
                            const std::string& val) {
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
MemcachedBackend::increase_size_impl(const std::string& key, size_t io_size,
                                   off_t offset, bool append) {
    //lock_guard<recursive_mutex> lock_guard(memcached_mutex_);
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
MemcachedBackend::decrease_size_impl(const std::string& key, size_t size) {
    //lock_guard<recursive_mutex> lock_guard(memcached_mutex_);
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
MemcachedBackend::get_dirents_impl(const std::string& dir) const {
    auto root_path = dir;
    std::vector<std::string> keys;
    std::vector<std::pair<std::string, bool>> entries;
    
    memcached_dump_fn dump_callback = [](const memcached_st*, const char* key, 
                                        size_t key_length, void* context) {
        std::vector<std::string>* keys = 
                                static_cast<std::vector<std::string>*>(context);
        std::string current_key(key, key_length);
        std::string root_path = keys->front();
        if(current_key.size() >= root_path.size() && 
        current_key.compare(0, root_path.size(), root_path) == 0)
            keys->push_back(current_key);
        return MEMCACHED_SUCCESS; 
    };

    keys.push_back(root_path);

    memcached_return_t rc;
    memcached_st *memc = memcached_pool_pop(mmc_pool_ , NULL , &rc); 
    if (rc != MEMCACHED_SUCCESS) {
        throw_status_excpt(memcached_strerror(memc, rc));
    }
    rc = memcached_dump(memc, &dump_callback, &keys, 1, true);
    memcached_pool_push(mmc_pool_ , memc);  
    if (rc != MEMCACHED_SUCCESS) {
        throw_status_excpt(memcached_strerror(memc, rc));
    }

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
MemcachedBackend::get_dirents_extended_impl(const std::string& dir) const {
    auto root_path = dir;
    std::vector<std::string> keys;
    std::vector<std::tuple<std::string, bool, size_t, time_t>> entries;

    memcached_dump_fn dump_callback = [](const memcached_st*, const char* key, 
                                        size_t key_length, void* context) {
        std::vector<std::string>* keys = 
                                static_cast<std::vector<std::string>*>(context);
        std::string current_key(key, key_length);
        std::string root_path = keys->back();
        if(current_key.size() >= root_path.size() && 
        current_key.compare(0, root_path.size(), root_path) == 0)
            keys->push_back(current_key);
        return MEMCACHED_SUCCESS; 
    };

    keys.push_back(root_path);

    memcached_return_t rc;
    memcached_st *memc = memcached_pool_pop(mmc_pool_ , NULL , &rc); 
    if (rc != MEMCACHED_SUCCESS) {
        throw_status_excpt(memcached_strerror(memc, rc));
    }
    rc = memcached_dump(memc, &dump_callback, &keys, 1, true);
    memcached_pool_push(mmc_pool_ , memc);  
    if (rc != MEMCACHED_SUCCESS) {
        throw_status_excpt(memcached_strerror(memc, rc));
    }

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
MemcachedBackend::iterate_all_impl() const {
    ;
}

/**
 * Used for setting KV store settings
 */
void
MemcachedBackend::optimize_database_impl() {
    ;
}

} // namespace gkfs::metadata
