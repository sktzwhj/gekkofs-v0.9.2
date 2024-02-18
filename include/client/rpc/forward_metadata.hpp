/*
  Copyright 2018-2024, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2024, Johannes Gutenberg Universitaet Mainz, Germany

  This software was partially supported by the
  EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).

  This software was partially supported by the
  ADA-FS project under the SPPEXA project funded by the DFG.

  This file is part of GekkoFS' POSIX interface.

  GekkoFS' POSIX interface is free software: you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public License as
  published by the Free Software Foundation, either version 3 of the License,
  or (at your option) any later version.

  GekkoFS' POSIX interface is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with GekkoFS' POSIX interface.  If not, see
  <https://www.gnu.org/licenses/>.

  SPDX-License-Identifier: LGPL-3.0-or-later
*/

#ifndef GEKKOFS_CLIENT_FORWARD_METADATA_HPP
#define GEKKOFS_CLIENT_FORWARD_METADATA_HPP

#include <string>
#include <memory>
#include <vector>
/* Forward declaration */
namespace gkfs {
namespace filemap {
class OpenDir;
}
namespace metadata {
struct MetadentryUpdateFlags;

class Metadata;
} // namespace metadata

// TODO once we have LEAF, remove all the error code returns and throw them as
// an exception.

namespace rpc {

int
forward_create(const std::string& path, mode_t mode);

int
forward_stat(const std::string& path, std::string& attr);

#ifdef HAS_RENAME
int
forward_rename(const std::string& oldpath, const std::string& newpath,
               const gkfs::metadata::Metadata& md);
#endif // HAS_RENAME

int
forward_remove(const std::string& path);

int
forward_decr_size(const std::string& path, size_t length);

int
forward_update_metadentry(
        const std::string& path, const gkfs::metadata::Metadata& md,
        const gkfs::metadata::MetadentryUpdateFlags& md_flags);

std::pair<int, off64_t>
forward_update_metadentry_size(const std::string& path, size_t size,
                               off64_t offset, bool append_flag);

std::pair<int, off64_t>
forward_get_metadentry_size(const std::string& path);

std::pair<int, std::shared_ptr<gkfs::filemap::OpenDir>>
forward_get_dirents(const std::string& path);

std::pair<int, std::vector<std::tuple<const std::string, bool, size_t, time_t>>>
forward_get_dirents_single(const std::string& path, int server);

#ifdef HAS_SYMLINKS

int
forward_mk_symlink(const std::string& path, const std::string& target_path);

#endif

} // namespace rpc
} // namespace gkfs

#endif // GEKKOFS_CLIENT_FORWARD_METADATA_HPP
