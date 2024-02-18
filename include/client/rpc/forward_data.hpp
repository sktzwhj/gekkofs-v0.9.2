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

#ifndef GEKKOFS_CLIENT_FORWARD_DATA_HPP
#define GEKKOFS_CLIENT_FORWARD_DATA_HPP

namespace gkfs::rpc {

struct ChunkStat {
    unsigned long chunk_size;
    unsigned long chunk_total;
    unsigned long chunk_free;
};

// TODO once we have LEAF, remove all the error code returns and throw them as
// an exception.

std::pair<int, ssize_t>
forward_write(const std::string& path, const void* buf, off64_t offset,
              size_t write_size);

std::pair<int, ssize_t>
forward_read(const std::string& path, void* buf, off64_t offset,
             size_t read_size);

int
forward_truncate(const std::string& path, size_t current_size, size_t new_size);

std::pair<int, ChunkStat>
forward_get_chunk_stat();

} // namespace gkfs::rpc

#endif // GEKKOFS_CLIENT_FORWARD_DATA_HPP
