/*
  Copyright 2018-2022, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2022, Johannes Gutenberg Universitaet Mainz, Germany

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

#include <daemon/ops/metadentry.hpp>
#include <daemon/backend/metadata/db.hpp>
#include <daemon/backend/data/chunk_storage.hpp>
#include <daemon/backend/metadata/metadata_module.hpp>

using namespace std;

namespace gkfs::metadata {

Metadata
get(const std::string& path) {
    return Metadata(get_str(path));
}

std::string
get_str(const std::string& path) {
    return GKFS_DATA->mdb()->get(path);
}

size_t
get_size(const string& path) {
    return get(path).size();
}

std::vector<std::pair<std::string, bool>>
get_dirents(const std::string& dir) {
    return GKFS_DATA->mdb()->get_dirents(dir);
}

std::vector<std::tuple<std::string, bool, size_t, time_t>>
get_dirents_extended(const std::string& dir) {
    return GKFS_DATA->mdb()->get_dirents_extended(dir);
}

void
create(const std::string& path, Metadata& md) {

    // update metadata object based on what metadata is needed
    if(GKFS_DATA->atime_state() || GKFS_DATA->mtime_state() ||
       GKFS_DATA->ctime_state()) {
        std::time_t time;
        std::time(&time);
        auto time_s = fmt::format_int(time).str();
        if(GKFS_DATA->atime_state())
            md.atime(time);
        if(GKFS_DATA->mtime_state())
            md.mtime(time);
        if(GKFS_DATA->ctime_state())
            md.ctime(time);
    }
    if constexpr(gkfs::config::metadata::create_exist_check) {
        GKFS_DATA->mdb()->put_no_exist(path, md.serialize());
    } else {
        GKFS_DATA->mdb()->put(path, md.serialize());
    }
}

void
update(const string& path, Metadata& md) {
    GKFS_DATA->mdb()->update(path, path, md.serialize());
}

/**
 * @internal
 * Updates a metadentry's size atomically and returns the starting offset
 * for the I/O operation.
 * This is primarily necessary for parallel write operations,
 * e.g., with O_APPEND, where the EOF might have changed since opening the file.
 * Therefore, we use update_size to assign a safe write interval to each
 * parallel write operation.
 * @endinternal
 */
off_t
update_size(const string& path, size_t io_size, off64_t offset, bool append) {
    return GKFS_DATA->mdb()->increase_size(path, io_size, offset, append);
}

void
remove(const string& path) {
    /*
     * try to remove metadata from kv store but catch NotFoundException which is
     * not an error in this case because removes can be broadcast to catch all
     * data chunks but only one node will hold the kv store entry.
     */
    try {
        GKFS_DATA->mdb()->remove(path); // remove metadata from KV store
    } catch(const NotFoundException& e) {
    }
}

} // namespace gkfs::metadata