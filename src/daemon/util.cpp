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

#include <daemon/util.hpp>
#include <daemon/daemon.hpp>

#include <common/rpc/rpc_util.hpp>

using namespace std;

namespace gkfs::utils {

/**
 * @internal
 * Appends a single line to an existing shared hosts file with the RPC
 * connection information of this daemon. If it doesn't exist, it is created.
 * The line includes the hostname (and rootdir_suffix if applicable) and the RPC
 * server's listening address.
 *
 * NOTE, the shared file system must support strong consistency semantics to
 * ensure each daemon can write its information to the file even if the write
 * access is simultaneous.
 * @endinternal
 */
void
populate_hosts_file() {
    const auto& hosts_file = GKFS_DATA->hosts_file();
    GKFS_DATA->spdlogger()->debug("{}() Populating hosts file: '{}'", __func__,
                                  hosts_file);
    ofstream lfstream(hosts_file, ios::out | ios::app);
    if(!lfstream) {
        throw runtime_error(fmt::format("Failed to open hosts file '{}': {}",
                                        hosts_file, strerror(errno)));
    }
    // if rootdir_suffix is used, append it to hostname
    auto hostname =
            GKFS_DATA->rootdir_suffix().empty()
                    ? gkfs::rpc::get_my_hostname(true)
                    : fmt::format("{}#{}", gkfs::rpc::get_my_hostname(true),
                                  GKFS_DATA->rootdir_suffix());
    lfstream << fmt::format("{} {}", hostname, RPC_DATA->self_addr_str())
             << std::endl;
    if(!lfstream) {
        throw runtime_error(
                fmt::format("Failed to write on hosts file '{}': {}",
                            hosts_file, strerror(errno)));
    }
    lfstream.close();
}

/**
 * @internal
 * This function removes the entire hosts file even if just one daemon is
 * shutdown. This makes sense because the data distribution calculation would be
 * misaligned if the entry of the current daemon was only removed.
 * @endinternal
 */
void
destroy_hosts_file() {
    std::remove(GKFS_DATA->hosts_file().c_str());
}

} // namespace gkfs::utils
