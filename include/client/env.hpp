/*
  Copyright 2018-2022, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2022, Johannes Gutenberg Universitaet Mainz, Germany

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

#ifndef GKFS_CLIENT_ENV
#define GKFS_CLIENT_ENV

#include <config.hpp>

#define ADD_PREFIX(str) CLIENT_ENV_PREFIX str

/* Environment variables for the GekkoFS client */
namespace gkfs::env {

static constexpr auto LOG = ADD_PREFIX("LOG");

#ifdef GKFS_DEBUG_BUILD
static constexpr auto LOG_DEBUG_VERBOSITY = ADD_PREFIX("LOG_DEBUG_VERBOSITY");
static constexpr auto LOG_SYSCALL_FILTER = ADD_PREFIX("LOG_SYSCALL_FILTER");
#endif

static constexpr auto LOG_OUTPUT = ADD_PREFIX("LOG_OUTPUT");
static constexpr auto LOG_PER_PROCESS = ADD_PREFIX("LOG_PER_PROCESS");
static constexpr auto LOG_OUTPUT_TRUNC = ADD_PREFIX("LOG_OUTPUT_TRUNC");
static constexpr auto CWD = ADD_PREFIX("CWD");
static constexpr auto HOSTS_FILE = ADD_PREFIX("HOSTS_FILE");
#ifdef GKFS_ENABLE_FORWARDING
static constexpr auto FORWARDING_MAP_FILE = ADD_PREFIX("FORWARDING_MAP_FILE");
#endif

} // namespace gkfs::env

#undef ADD_PREFIX

#endif // GKFS_CLIENT_ENV
