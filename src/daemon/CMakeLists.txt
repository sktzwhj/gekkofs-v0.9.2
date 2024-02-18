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

add_subdirectory(backend)

# ##############################################################################
# This builds the `gkfs_daemon` executable: the primary GekkoFS daemon.
# ##############################################################################
add_executable(gkfs_daemon)

pkg_check_modules(URING IMPORTED_TARGET liburing)

# We need to add here any files that may have different compile definitions
target_sources(
  gkfs_daemon
  PRIVATE daemon.cpp
          handler/srv_data.cpp
          ../common/rpc/rpc_util.cpp
          util.cpp
          ops/metadentry.cpp
          ops/data.cpp
          classes/fs_data.cpp
          classes/rpc_data.cpp
          handler/srv_metadata.cpp
          handler/srv_management.cpp
  PUBLIC ${CMAKE_SOURCE_DIR}/include/config.hpp
         ${CMAKE_SOURCE_DIR}/include/version.hpp.in
)
target_link_libraries(
  gkfs_daemon
  PUBLIC # internal libs
         metadata
         metadata_backend
         storage
         distributor
         statistics
         log_util
         env_util
         path_util
         # external libs
         CLI11::CLI11
         fmt::fmt
         Mercury::Mercury
         Argobots::Argobots
         Margo::Margo
         # others
         Threads::Threads
)

if(GKFS_ENABLE_CODE_COVERAGE)
    target_code_coverage(gkfs_daemon AUTO)
endif()

install(TARGETS gkfs_daemon RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# ##############################################################################
# This builds the `gkfwd_daemon` executable: the daemon for GekkoFS data
# forwarding mode.
# ##############################################################################

if(GKFS_ENABLE_FORWARDING)

  add_executable(gkfwd_daemon)

  # We need to add here any files that may have different compile definitions
  target_sources(
    gkfwd_daemon
    PRIVATE ../common/rpc/rpc_util.cpp
            daemon.cpp
            util.cpp
            ops/metadentry.cpp
            ops/data.cpp
            classes/fs_data.cpp
            classes/rpc_data.cpp
            handler/srv_metadata.cpp
            handler/srv_management.cpp
            handler/srv_data.cpp
  )

  target_compile_definitions(gkfwd_daemon PUBLIC GKFS_ENABLE_FORWARDING)
  target_link_libraries(
    gkfwd_daemon
    PUBLIC # internal libs
           metadata
           metadata_backend
           storage
           distributor
           statistics
           log_util
           env_util
           path_util
           # external libs
           CLI11::CLI11
           fmt::fmt
           Mercury::Mercury
           Argobots::Argobots
           Margo::Margo
           # others
           Threads::Threads
  )

  if(GKFS_ENABLE_AGIOS)
    target_sources(gkfwd_daemon PRIVATE scheduler/agios.cpp)
    target_compile_definitions(gkfwd_daemon PUBLIC GKFS_ENABLE_AGIOS)
    target_link_libraries(gkfwd_daemon PRIVATE AGIOS::AGIOS)
  endif()

  install(TARGETS gkfwd_daemon RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif ()
