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

#ifndef GEKKOFS_METADENTRY_HPP
#define GEKKOFS_METADENTRY_HPP

#include <daemon/daemon.hpp>
#include <common/metadata.hpp>

namespace gkfs::metadata {

/**
 * @brief Returns the metadata of an object at a specific path. The metadata can
 * be of dummy values if configured
 * @param path
 * @param attr
 * @return
 */
Metadata
get(const std::string& path);

/**
 * @brief Get metadentry string only for path
 * @param path
 * @return
 */
std::string
get_str(const std::string& path);

/**
 * @brief Gets the size of a metadentry
 * @param path
 * @param ret_size (return val)
 * @return err
 */
size_t
get_size(const std::string& path);

/**
 * @brief Returns a vector of directory entries for given directory
 * @param dir
 * @return
 */
std::vector<std::pair<std::string, bool>>
get_dirents(const std::string& dir);

/**
 * @brief Returns a vector of directory entries for given directory (extended
 * version)
 * @param dir
 * @return
 */
std::vector<std::tuple<std::string, bool, size_t, time_t>>
get_dirents_extended(const std::string& dir);

/**
 * @brief Creates metadata (if required) and dentry at the same time
 * @param path
 * @param mode
 * @throws DBException
 */
void
create(const std::string& path, Metadata& md);

/**
 * @brief Update metadentry by given Metadata object and path
 * @param path
 * @param md
 */
void
update(const std::string& path, Metadata& md);

/**
 * @brief Updates a metadentry's size atomically and returns the starting offset
 * for the I/O operation.
 * @param path
 * @param io_size
 * @param offset
 * @param append
 * @return starting offset for I/O operation
 */
off_t
update_size(const std::string& path, size_t io_size, off_t offset, bool append);

/**
 * @brief Remove metadentry if exists
 * @param path
 * @return
 * @throws gkfs::metadata::DBException
 */
void
remove(const std::string& path);

} // namespace gkfs::metadata

#endif // GEKKOFS_METADENTRY_HPP
