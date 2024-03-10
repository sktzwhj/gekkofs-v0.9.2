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

#ifndef GEKKOFS_COMMON_RPC_UTILS_HPP
#define GEKKOFS_COMMON_RPC_UTILS_HPP

extern "C" {
#include <mercury_types.h>
#include <mercury_proc_string.h>
}

#include <string>
#include <iostream>
#include <sstream>
#include <vector>

namespace gkfs::rpc {

hg_bool_t
bool_to_merc_bool(bool state);

std::string
get_my_hostname(bool short_hostname = false);

#ifdef GKFS_ENABLE_UNUSED_FUNCTIONS
std::string
get_host_by_name(const std::string& hostname);
#endif

bool
get_bitset(const std::vector<uint8_t>& data, const uint16_t position);

void
set_bitset(std::vector<uint8_t>& data, const uint16_t position);

std::string
compress_bitset(const std::vector<uint8_t>& bytes);

std::vector<uint8_t>
decompress_bitset(const std::string& compressedString);

} // namespace gkfs::rpc

#endif // GEKKOFS_COMMON_RPC_UTILS_HPP
