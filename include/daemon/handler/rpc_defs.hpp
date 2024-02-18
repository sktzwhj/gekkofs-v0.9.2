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
/**
 * @brief Declare all Margo RPC handler functions by name that
 * were registered in the daemon's main source file.
 */

#ifndef GKFS_DAEMON_RPC_DEFS_HPP
#define GKFS_DAEMON_RPC_DEFS_HPP

extern "C" {
#include <margo.h>
}

/* visible API for RPC operations */

DECLARE_MARGO_RPC_HANDLER(rpc_srv_get_fs_config)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_create)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_stat)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_decr_size)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_remove_metadata)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_update_metadentry)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_get_metadentry_size)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_update_metadentry_size)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_get_dirents)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_get_dirents_extended)
#ifdef HAS_SYMLINKS

DECLARE_MARGO_RPC_HANDLER(rpc_srv_mk_symlink)

#endif


// data
DECLARE_MARGO_RPC_HANDLER(rpc_srv_remove_data)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_read)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_write)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_truncate)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_get_chunk_stat)

#endif // GKFS_DAEMON_RPC_DEFS_HPP
