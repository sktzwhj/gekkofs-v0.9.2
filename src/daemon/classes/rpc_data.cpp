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

#include <daemon/classes/rpc_data.hpp>

using namespace std;

namespace gkfs {

namespace daemon {

// Getter/Setter

margo_instance*
RPCData::server_rpc_mid() {
    return server_rpc_mid_;
}

void
RPCData::server_rpc_mid(margo_instance* server_rpc_mid) {
    RPCData::server_rpc_mid_ = server_rpc_mid;
}

ABT_pool
RPCData::io_pool() const {
    return io_pool_;
}

void
RPCData::io_pool(ABT_pool io_pool) {
    RPCData::io_pool_ = io_pool;
}

vector<ABT_xstream>&
RPCData::io_streams() {
    return io_streams_;
}

void
RPCData::io_streams(const vector<ABT_xstream>& io_streams) {
    RPCData::io_streams_ = io_streams;
}

const std::string&
RPCData::self_addr_str() const {
    return self_addr_str_;
}

void
RPCData::self_addr_str(const std::string& addr_str) {
    self_addr_str_ = addr_str;
}

const std::shared_ptr<gkfs::rpc::Distributor>&
RPCData::distributor() const {
    return distributor_;
}

void
RPCData::distributor(
        const std::shared_ptr<gkfs::rpc::Distributor>& distributor) {
    distributor_ = distributor;
}


} // namespace daemon
} // namespace gkfs
