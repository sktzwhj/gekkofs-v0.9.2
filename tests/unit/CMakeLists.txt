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

# get Catch2
set(FETCHCONTENT_QUIET OFF)
FetchContent_Declare(catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG 216713a4066b79d9803d374f261ccb30c0fb451f # v2.13.8
    GIT_SHALLOW ON
    GIT_PROGRESS ON
    )

FetchContent_GetProperties(catch2)

if (NOT catch2_POPULATED)
    FetchContent_Populate(catch2)
    message(STATUS "[gkfs] Catch2 source dir: ${catch2_SOURCE_DIR}")
    message(STATUS "[gkfs] Catch2 binary dir: ${catch2_BINARY_DIR}")
    set(CATCH_BUILD_TESTING OFF CACHE INTERNAL "")
    add_subdirectory(${catch2_SOURCE_DIR} ${catch2_BINARY_DIR})
endif ()

add_subdirectory(helpers)

# create a convenience library with Catch2's main 
# to speed up test compilation
add_library(catch2_main STATIC)
target_sources(catch2_main PRIVATE catch_main.cpp)
target_link_libraries(catch2_main
    Catch2::Catch2
    )

# define executables for tests and make them depend on the convenience
# library (and Catch2 transitively) and fmt
add_executable(tests)
target_sources(tests
    PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/test_utils_arithmetic.cpp
    ${CMAKE_CURRENT_LIST_DIR}/test_helpers.cpp)

if(GKFS_TESTS_GUIDED_DISTRIBUTION)
    target_sources(tests PRIVATE ${CMAKE_CURRENT_LIST_DIR}/test_guided_distributor.cpp)
endif()

target_link_libraries(tests
    PRIVATE
    catch2_main
    fmt::fmt
    helpers
    arithmetic
    distributor
    )

# Catch2's contrib folder includes some helper functions
# to auto-discover Catch tests and register them in CTest
set(CMAKE_MODULE_PATH "${catch2_SOURCE_DIR}/contrib" ${CMAKE_MODULE_PATH})
include(Catch)
catch_discover_tests(tests
    PROPERTIES LABELS "unit::all"
    )

if (GKFS_INSTALL_TESTS)
    install(TARGETS tests
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/unit
        )
endif ()
