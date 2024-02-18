################################################################################
# Copyright 2018-2024, Barcelona Supercomputing Center (BSC), Spain            #
# Copyright 2015-2024, Johannes Gutenberg Universitaet Mainz, Germany          #
#                                                                              #
# This software was partially supported by the                                 #
# EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).    #
#                                                                              #
# This software was partially supported by the                                 #
# ADA-FS project under the SPPEXA project funded by the DFG.                   #
#                                                                              #
# This file is part of GekkoFS.                                                #
#                                                                              #
# GekkoFS is free software: you can redistribute it and/or modify              #
# it under the terms of the GNU General Public License as published by         #
# the Free Software Foundation, either version 3 of the License, or            #
# (at your option) any later version.                                          #
#                                                                              #
# GekkoFS is distributed in the hope that it will be useful,                   #
# but WITHOUT ANY WARRANTY; without even the implied warranty of               #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                #
# GNU General Public License for more details.                                 #
#                                                                              #
# You should have received a copy of the GNU General Public License            #
# along with GekkoFS.  If not, see <https://www.gnu.org/licenses/>.            #
#                                                                              #
# SPDX-License-Identifier: GPL-3.0-or-later                                    #
################################################################################

# common sources to both gkfs_intercept and gkfwd_intercept

target_sources(
  gkfs_intercept
  PUBLIC gkfs_functions.hpp
         env.hpp
         hooks.hpp
         intercept.hpp
         logging.hpp
         make_array.hpp
         open_file_map.hpp
         open_dir.hpp
         path.hpp
         preload.hpp
         preload_context.hpp
         preload_util.hpp
         rpc/rpc_types.hpp
         rpc/forward_management.hpp
         rpc/forward_metadata.hpp
         rpc/forward_data.hpp
         syscalls/args.hpp
         syscalls/decoder.hpp
         syscalls/errno.hpp
         syscalls/rets.hpp
         syscalls/syscall.hpp
         syscalls/detail/syscall_info.h
)

if(GKFS_ENABLE_FORWARDING)
  target_sources(
    gkfwd_intercept
    PUBLIC gkfs_functions.hpp
           env.hpp
           hooks.hpp
           intercept.hpp
           logging.hpp
           make_array.hpp
           open_file_map.hpp
           open_dir.hpp
           path.hpp
           preload.hpp
           preload_context.hpp
           preload_util.hpp
           rpc/rpc_types.hpp
           rpc/forward_management.hpp
           rpc/forward_metadata.hpp
           rpc/forward_data.hpp
           syscalls/args.hpp
           syscalls/decoder.hpp
           syscalls/errno.hpp
           syscalls/rets.hpp
           syscalls/syscall.hpp
           syscalls/detail/syscall_info.h
  )
endif()
