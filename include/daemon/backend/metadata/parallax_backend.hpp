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

#ifndef GEKKOFS_METADATA_PARALLAXBACKEND_HPP
#define GEKKOFS_METADATA_PARALLAXBACKEND_HPP

#include <memory>
#include <spdlog/spdlog.h>
#include <daemon/backend/exceptions.hpp>
#include <tuple>
#include <cstdio>
extern "C" {
#include <parallax.h>
}

namespace gkfs::metadata {

class ParallaxBackend : public MetadataBackend<ParallaxBackend> {
private:
    par_handle par_db_;
    par_db_options par_options_;
    std::string par_path_;

    /**
     * Convert a String to klc_key
     * @param key
     * @param klc_key struct
     */
    inline void
    str2par(const std::string& value, struct par_value& V) const;


    /**
     * Convert a String to klc_value
     * @param value
     * @param klc_value struct
     */
    inline void
    str2par(const std::string& key, struct par_key& K) const;

public:
    /**
     * Called when the daemon is started: Connects to the KV store
     * @param path where KV store data is stored
     */
    explicit ParallaxBackend(const std::string& path);

    /**
     * Exception wrapper on Status object. Throws NotFoundException if
     * s == "Not Found", general DBException otherwise
     * @param String with status
     * @throws DBException
     */
    static inline void
    throw_status_excpt(const std::string& s);

    /**
     * @brief Destroy the Kreon Backend:: Kreon Backend object
     * We remove the file, too large for the CI.
     * TODO: Insert option
     */
    virtual ~ParallaxBackend();

    /**
     * Gets a KV store value for a key
     * @param key
     * @return value
     * @throws DBException on failure, NotFoundException if entry doesn't exist
     */
    std::string
    get_impl(const std::string& key) const;

    /**
     * Puts an entry into the KV store
     * @param key
     * @param val
     * @throws DBException on failure
     */
    void
    put_impl(const std::string& key, const std::string& val);

    /**
     * Puts an entry into the KV store if it doesn't exist. This function does
     * not use a mutex.
     * @param key
     * @param val
     * @throws DBException on failure, ExistException if entry already exists
     */
    void
    put_no_exist_impl(const std::string& key, const std::string& val);

    /**
     * Removes an entry from the KV store
     * @param key
     * @throws DBException on failure, NotFoundException if entry doesn't exist
     */
    void
    remove_impl(const std::string& key);

    /**
     * checks for existence of an entry
     * @param key
     * @return true if exists
     * @throws DBException on failure
     */
    bool
    exists_impl(const std::string& key);

    /**
     * Updates a metadentry atomically and also allows to change keys
     * @param old_key
     * @param new_key
     * @param val
     * @throws DBException on failure, NotFoundException if entry doesn't exist
     */
    void
    update_impl(const std::string& old_key, const std::string& new_key,
                const std::string& val);

    /**
     * Updates the size on the metadata
     * Operation. E.g., called before a write() call
     * @param key
     * @param io_size
     * @param offset
     * @param append
     * @return offset where the write operation should start. This is only used
     * when append is set
     */
    off_t
    increase_size_impl(const std::string& key, size_t io_size, off_t offset,
                       bool append);

    /**
     * Decreases the size on the metadata
     * Operation E.g., called before a truncate() call
     * @param key
     * @param size
     * @throws DBException on failure
     */
    void
    decrease_size_impl(const std::string& key, size_t size);

    /**
     * Return all the first-level entries of the directory @dir
     *
     * @return vector of pair <std::string name, bool is_dir>,
     *         where name is the name of the entries and is_dir
     *         is true in the case the entry is a directory.
     */
    std::vector<std::pair<std::string, bool>>
    get_dirents_impl(const std::string& dir) const;

    /**
     * Return all the first-level entries of the directory @dir
     *
     * @return vector of pair <std::string name, bool is_dir - size - ctime>,
     *         where name is the name of the entries and is_dir
     *         is true in the case the entry is a directory.
     */
    std::vector<std::tuple<std::string, bool, size_t, time_t>>
    get_dirents_extended_impl(const std::string& dir) const;

    /**
     * Code example for iterating all entries in KV store. This is for debug
     * only as it is too expensive
     */
    void
    iterate_all_impl() const;
};

} // namespace gkfs::metadata

#endif // GEKKOFS_METADATA_KREONBACKEND_HPP
