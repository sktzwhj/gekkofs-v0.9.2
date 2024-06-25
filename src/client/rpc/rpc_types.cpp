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

#include <hermes.hpp>
#include <client/rpc/rpc_types.hpp>

//==============================================================================
// register request types so that they can be used by users and the engine
//
void
hermes::detail::register_user_request_types() {
    (void) registered_requests().add<gkfs::rpc::fs_config>();
    (void) registered_requests().add<gkfs::rpc::registry_request>();// --Multiple GekkoFS--
    (void) registered_requests().add<gkfs::rpc::registry_register>();// --Multiple GekkoFS--
    (void) registered_requests().add<gkfs::rpc::create>();
    (void) registered_requests().add<gkfs::rpc::stat>();
    (void) registered_requests().add<gkfs::rpc::remove_metadata>();
    (void) registered_requests().add<gkfs::rpc::decr_size>();
    (void) registered_requests().add<gkfs::rpc::update_metadentry>();
    (void) registered_requests().add<gkfs::rpc::get_metadentry_size>();
    (void) registered_requests().add<gkfs::rpc::update_metadentry_size>();

#ifdef HAS_SYMLINKS
    (void) registered_requests().add<gkfs::rpc::mk_symlink>();
#endif // HAS_SYMLINKS
    (void) registered_requests().add<gkfs::rpc::remove_data>();
    (void) registered_requests().add<gkfs::rpc::write_data>();
    (void) registered_requests().add<gkfs::rpc::read_data>();
    (void) registered_requests().add<gkfs::rpc::trunc_data>();
    (void) registered_requests().add<gkfs::rpc::get_dirents>();
    (void) registered_requests().add<gkfs::rpc::chunk_stat>();
    (void) registered_requests().add<gkfs::rpc::get_dirents_extended>();
}
