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

#include <client/rpc/forward_management.hpp>
#include <client/logging.hpp>
#include <client/preload_util.hpp>
#include <client/rpc/rpc_types.hpp>

namespace gkfs::rpc {

/**
 * Gets fs configuration information from the running daemon and transfers it to
 * the memory of the library
 * @return
 */
bool
forward_get_fs_config() {

    auto endp = CTX->hosts().at(CTX->local_host_id());
    gkfs::rpc::fs_config::output out;

    try {
        LOG(DEBUG, "Retrieving file system configurations from daemon");
        // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
        // can retry for RPC_TRIES (see old commits with margo)
        // TODO(amiranda): hermes will eventually provide a post(endpoint)
        // returning one result and a broadcast(endpoint_set) returning a
        // result_set. When that happens we can remove the .at(0) :/
        out = ld_network_service->post<gkfs::rpc::fs_config>(endp).get().at(0);
    } catch(const std::exception& ex) {
        LOG(ERROR, "Retrieving fs configurations from daemon");
        return false;
    }

    CTX->mountdir(out.mountdir());
    LOG(INFO, "Mountdir: '{}'", CTX->mountdir());

    CTX->fs_conf()->rootdir = out.rootdir();
    CTX->fs_conf()->atime_state = out.atime_state();
    CTX->fs_conf()->mtime_state = out.mtime_state();
    CTX->fs_conf()->ctime_state = out.ctime_state();
    CTX->fs_conf()->link_cnt_state = out.link_cnt_state();
    CTX->fs_conf()->blocks_state = out.blocks_state();
    CTX->fs_conf()->uid = out.uid();
    CTX->fs_conf()->gid = out.gid();

    LOG(DEBUG, "Got response with mountdir {}", out.mountdir());

    return true;
}

} // namespace gkfs::rpc
