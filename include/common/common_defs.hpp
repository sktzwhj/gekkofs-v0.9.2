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

#ifndef GEKKOFS_COMMON_DEFS_HPP
#define GEKKOFS_COMMON_DEFS_HPP

// These constexpr set the RPC's identity and which handler the receiver end
// should use
namespace gkfs::rpc {

using chnk_id_t = unsigned long;

namespace tag {

constexpr auto fs_config = "rpc_srv_fs_config";
constexpr auto create = "rpc_srv_mk_node";
constexpr auto stat = "rpc_srv_stat";
constexpr auto remove_metadata = "rpc_srv_rm_metadata";
constexpr auto remove_data = "rpc_srv_rm_data";
constexpr auto decr_size = "rpc_srv_decr_size";
constexpr auto update_metadentry = "rpc_srv_update_metadentry";
constexpr auto get_metadentry_size = "rpc_srv_get_metadentry_size";
constexpr auto update_metadentry_size = "rpc_srv_update_metadentry_size";
constexpr auto get_dirents = "rpc_srv_get_dirents";
constexpr auto get_dirents_extended = "rpc_srv_get_dirents_extended";
#ifdef HAS_SYMLINKS
constexpr auto mk_symlink = "rpc_srv_mk_symlink";
#endif
constexpr auto write = "rpc_srv_write_data";
constexpr auto read = "rpc_srv_read_data";
constexpr auto truncate = "rpc_srv_trunc_data";
constexpr auto get_chunk_stat = "rpc_srv_chunk_stat";
} // namespace tag

namespace protocol {
constexpr auto ofi_psm2 = "ofi+psm2";
constexpr auto ofi_sockets = "ofi+sockets";
constexpr auto ofi_tcp = "ofi+tcp";
constexpr auto ofi_verbs = "ofi+verbs";
constexpr auto na_sm = "na+sm";
} // namespace protocol
} // namespace gkfs::rpc

namespace gkfs::config::stats {
/**
 * Number 512-byte blocks allocated as it is in the linux kernel (struct_stat.h)
 */
constexpr auto st_nblocksize = 512;
} // namespace gkfs::config::stats

#endif // GEKKOFS_COMMON_DEFS_HPP
