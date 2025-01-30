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

# Define metadata module library first
add_library(metadata_module STATIC)
target_sources(
  metadata_module
  PUBLIC ${CMAKE_SOURCE_DIR}/include/daemon/backend/metadata/metadata_module.hpp
  PRIVATE metadata_module.cpp
)
target_link_libraries(metadata_module PRIVATE log_util)

# Define metadata_backend and its common dependencies and sources
add_library(metadata_backend STATIC)
target_sources(
  metadata_backend
  PUBLIC
    ${CMAKE_SOURCE_DIR}/include/daemon/backend/metadata/db.hpp
    ${CMAKE_SOURCE_DIR}/include/daemon/backend/exceptions.hpp
    ${CMAKE_SOURCE_DIR}/include/daemon/backend/metadata/metadata_backend.hpp
  PRIVATE ${CMAKE_SOURCE_DIR}/include/daemon/backend/metadata/merge.hpp
          merge.cpp db.cpp
)

target_link_libraries(
  metadata_backend
  PRIVATE metadata_module dl log_util path_util
)

if(GKFS_ENABLE_ROCKSDB)
  target_sources(
    metadata_backend
    PUBLIC ${CMAKE_SOURCE_DIR}/include/daemon/backend/metadata/rocksdb_backend.hpp
    PRIVATE rocksdb_backend.cpp
  )

  # If liburing is available in the system, RocksDB will have been built
  # with liburing support. If so, we need to propagate the dependency to
  # the daemon. Since liburing exports a pkg-config .pc file, we can use it
  # to retrieve its details.
  pkg_check_modules(URING IMPORTED_TARGET liburing)

  if(URING_FOUND)
    target_link_libraries(metadata_backend PUBLIC PkgConfig::URING)
  endif()

  target_link_libraries(metadata_backend PUBLIC RocksDB::rocksdb)
endif()

if(GKFS_ENABLE_PARALLAX)
  target_sources(metadata_backend
    PUBLIC ${CMAKE_SOURCE_DIR}/include/daemon/backend/metadata/parallax_backend.hpp
    PRIVATE parallax_backend.cpp
    )
  target_link_libraries(metadata_backend PUBLIC Parallax::parallax
    Parallax::log)
endif()

if(GKFS_ENABLE_REDIS)
  target_sources(metadata_backend
    PUBLIC ${CMAKE_SOURCE_DIR}/include/daemon/backend/metadata/redis_backend.hpp
    PRIVATE redis_backend.cpp
    )
  target_link_libraries(metadata_backend PUBLIC ${HIREDIS_LIB} ${REDIS_PLUS_PLUS_LIB})
endif()

if(GKFS_ENABLE_MEMCACHED)
  target_sources(metadata_backend
    PUBLIC ${CMAKE_SOURCE_DIR}/include/daemon/backend/metadata/memcached_backend.hpp
    PRIVATE memcached_backend.cpp
    )
  target_link_libraries(metadata_backend PUBLIC ${LIBEVENT_LIB} ${LIBMEMCACHED_LIB})
endif()
