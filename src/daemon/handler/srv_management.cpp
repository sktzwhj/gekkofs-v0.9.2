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
 * @brief Provides all Margo RPC handler definitions called by Mercury on client
 * request for all file system management operations.
 * @internal
 * The end of the file defines the associates the Margo RPC handler functions
 * and associates them with their corresponding GekkoFS handler functions.
 * @endinternal
 */
#include <daemon/daemon.hpp>
#include <daemon/handler/rpc_defs.hpp>

#include <common/rpc/rpc_types.hpp>

extern "C" {
#include <unistd.h>
}

using namespace std;

namespace {

/**
 * @brief Responds with general file system meta information requested on client
 * startup.
 * @internal
 * Most notably this is where the client gets the information on which path
 * GekkoFS is accessible.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_get_fs_config(hg_handle_t handle) {
    rpc_config_out_t out{};

    GKFS_DATA->spdlogger()->debug("{}() Got config RPC", __func__);

    // get fs config
    out.mountdir = GKFS_DATA->mountdir().c_str();
    out.rootdir = GKFS_DATA->rootdir().c_str();
    out.atime_state = static_cast<hg_bool_t>(GKFS_DATA->atime_state());
    out.mtime_state = static_cast<hg_bool_t>(GKFS_DATA->mtime_state());
    out.ctime_state = static_cast<hg_bool_t>(GKFS_DATA->ctime_state());
    out.link_cnt_state = static_cast<hg_bool_t>(GKFS_DATA->link_cnt_state());
    out.blocks_state = static_cast<hg_bool_t>(GKFS_DATA->blocks_state());
    out.uid = getuid();
    out.gid = getgid();
    GKFS_DATA->spdlogger()->debug("{}() Sending output configs back to library",
                                  __func__);
    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to respond to client to serve file system configurations",
                __func__);
    }

    // Destroy handle when finished
    margo_destroy(handle);
    return HG_SUCCESS;
}

} // namespace

DEFINE_MARGO_RPC_HANDLER(rpc_srv_get_fs_config)
