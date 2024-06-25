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

#ifndef GEKKOFS_CLIENT_FORWARD_MNGMNT_HPP
#define GEKKOFS_CLIENT_FORWARD_MNGMNT_HPP
#include <string>
namespace gkfs::rpc {

bool
forward_get_fs_config();

/* --Multiple GekkoFS-- */

int
forward_request_registry(std::string flows, std::string hcfile, std::string hfile);

int
forward_register_registry(std::string work_flow, std::string hcfile, std::string hfile);
/* --Multiple GekkoFS-- */

} // namespace gkfs::rpc

#endif // GEKKOFS_CLIENT_FORWARD_MNGMNT_HPP
