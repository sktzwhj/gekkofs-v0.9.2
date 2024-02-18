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
 * @brief The main header file defining singletons and including needed headers
 * in the daemon.
 */
#ifndef GKFS_DAEMON_DAEMON_HPP
#define GKFS_DAEMON_DAEMON_HPP

// std libs
#include <string>
#include <spdlog/spdlog.h>

#include <config.hpp>
#include <common/common_defs.hpp>
// margo
extern "C" {
#include <abt.h>
#include <mercury.h>
#include <margo.h>
}

#include <daemon/classes/fs_data.hpp>
#include <daemon/classes/rpc_data.hpp>
#include <common/rpc/distributor.hpp>

#define GKFS_DATA                                                              \
    (static_cast<gkfs::daemon::FsData*>(                                       \
            gkfs::daemon::FsData::getInstance())) ///< GKFS_DATA macro to access
                                                  ///< the FsData singleton
                                                  ///< across the daemon
#define RPC_DATA                                                               \
    (static_cast<gkfs::daemon::RPCData*>(                                      \
            gkfs::daemon::RPCData::getInstance())) ///< RPC_DATA macro to access
                                                   ///< the RPCData singleton
                                                   ///< across the daemon

#endif // GKFS_DAEMON_DAEMON_HPP
