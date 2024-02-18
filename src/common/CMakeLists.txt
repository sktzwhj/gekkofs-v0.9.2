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

include(FetchContent)

add_library(rpc_utils OBJECT)
set_property(TARGET rpc_utils PROPERTY POSITION_INDEPENDENT_CODE ON)
target_sources(rpc_utils
  PRIVATE
    rpc/rpc_util.cpp
)

target_link_libraries(rpc_utils PUBLIC Mercury::Mercury)

add_subdirectory(arithmetic)

add_library(distributor STATIC)
set_property(TARGET distributor PROPERTY POSITION_INDEPENDENT_CODE ON)
target_sources(distributor
    PUBLIC
    ${INCLUDE_DIR}/common/rpc/distributor.hpp
    PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/rpc/distributor.cpp
    )

add_library(statistics STATIC)
set_property(TARGET statistics PROPERTY POSITION_INDEPENDENT_CODE ON)
target_sources(statistics
    PUBLIC
    ${INCLUDE_DIR}/common/statistics/stats.hpp
    PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/statistics/stats.cpp
    )


if(GKFS_ENABLE_PROMETHEUS)
  target_link_libraries(statistics
    PRIVATE
    prometheus-cpp::pull
    prometheus-cpp::push
    prometheus-cpp::core
    curl
    )
endif()

add_library(log_util STATIC)
set_property(TARGET log_util PROPERTY POSITION_INDEPENDENT_CODE ON)
target_sources(log_util
    PUBLIC
    ${INCLUDE_DIR}/common/log_util.hpp
    PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/log_util.cpp
    )
target_link_libraries(log_util
  PUBLIC
    spdlog::spdlog
    )

add_library(env_util STATIC)
set_property(TARGET env_util PROPERTY POSITION_INDEPENDENT_CODE ON)
target_sources(env_util
    PUBLIC
    ${INCLUDE_DIR}/common/env_util.hpp
    PRIVATE
    ${INCLUDE_DIR}/config.hpp
    ${CMAKE_CURRENT_LIST_DIR}/env_util.cpp
    )

add_library(metadata STATIC)
set_property(TARGET metadata PROPERTY POSITION_INDEPENDENT_CODE ON)
target_sources(metadata
    PUBLIC
    ${INCLUDE_DIR}/common/metadata.hpp
    PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/metadata.cpp
    )
target_link_libraries(metadata
    PRIVATE
    fmt::fmt
    )

add_library(path_util
    STATIC
    )
set_property(TARGET path_util PROPERTY POSITION_INDEPENDENT_CODE ON)

target_sources(path_util
    PRIVATE
    path_util.cpp
    PUBLIC
    ${INCLUDE_DIR}/common/path_util.hpp
    )
