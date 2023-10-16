################################################################################
# Copyright 2018-2022, Barcelona Supercomputing Center (BSC), Spain            #
# Copyright 2015-2022, Johannes Gutenberg Universitaet Mainz, Germany          #
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

# 3.13+ so that we have a sensible handling of relative paths
cmake_minimum_required(VERSION 3.13)

project(
    GekkoFS
    VERSION 0.9.1
)

enable_testing()

if (NOT CMAKE_COMPILER_IS_GNUCC)
    message(FATAL_ERROR "The choosen C compiler is not gcc and is not supported")
endif ()
if (NOT CMAKE_COMPILER_IS_GNUCXX)
    message(FATAL_ERROR "The choosen C++ compiler is not g++ and is not supported")
endif ()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release
        CACHE STRING "Choose the type of build: Debug Release Memcheck" FORCE)
ENDIF (NOT CMAKE_BUILD_TYPE)
message(STATUS "[${PROJECT_NAME}] Build type: ${CMAKE_BUILD_TYPE}")

# Compiler flags for various cmake build types
set(WARNINGS_FLAGS "-Wall -Wextra --pedantic -Wno-unused-parameter -Wno-missing-field-initializers")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG -O3")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${WARNINGS_FLAGS} -g -O0 -DGKFS_DEBUG_BUILD -DHERMES_DEBUG_BUILD")
set(CMAKE_CXX_FLAGS_MEMCHECK "${WARNINGS_FLAGS} -g -O0 -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_MAINTAINER "${WARNINGS_FLAGS} -g -O0 -pg -no-pie")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -DNDEBUG -O3")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${WARNINGS_FLAGS} -g -O0 -DGKFS_DEBUG_BUILD")
set(CMAKE_C_FLAGS_MEMCHECK "${WARNINGS_FLAGS} -g -O0 -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_C_FLAGS_MAINTAINER "${WARNINGS_FLAGS} -g -O0 -pg -no-pie")
mark_as_advanced(CMAKE_CXX_FLAGS_MAINTAINER)

# CMake and general includes
include(CheckCXXSourceCompiles)
include(CMakeDependentOption)
include(GNUInstallDirs)
include(FeatureSummary)

# Project version
set(GIT_VERSION_FOUND FALSE)
if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
    find_package(Git)
    if (Git_FOUND)
        execute_process(COMMAND /bin/bash -c "${GIT_EXECUTABLE} describe --tags --match='v*.*.*' --long --dirty"
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            RESULT_VARIABLE GIT_RET
            OUTPUT_VARIABLE GIT_VERSION
            ERROR_VARIABLE GIT_ERR
            OUTPUT_STRIP_TRAILING_WHITESPACE
            )
        if ("${GIT_RET}" STREQUAL "0")
            string(REGEX MATCH
                "^v([0-9]+)\.([0-9]+)\.([0-9]+)-([0-9]+)-(.*)$"
                DROP ${GIT_VERSION})
            set(PROJECT_VERSION_MAJOR ${CMAKE_MATCH_1})
            set(PROJECT_VERSION_MINOR ${CMAKE_MATCH_2})
            set(PROJECT_VERSION_PATCH ${CMAKE_MATCH_3})
            set(GKFS_COMMIT_GAP ${CMAKE_MATCH_4})
            set(GKFS_COMMIT_HASH ${CMAKE_MATCH_5})
            set(PROJECT_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}-snapshot+${GKFS_COMMIT_GAP}-${GKFS_COMMIT_HASH}")
            set(GIT_VERSION_FOUND TRUE)
        else ()
            message(WARNING "Failed to run `git describe`: [${GIT_RET}] '${GIT_ERR}'")
        endif ()
    endif ()
endif ()
message(STATUS "[${PROJECT_NAME}] Project version: ${PROJECT_VERSION}")
set(GKFS_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(GKFS_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(GKFS_VERSION_PATCH ${PROJECT_VERSION_PATCH})
set(GKFS_VERSION_STRING ${PROJECT_VERSION})
configure_file(include/version.hpp.in include/version.hpp)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})

# Import some convenience functions
include(gkfs-utils)

################################################################################
## Configuration option definitions:
## =================================
##
## The `gkfs-options` module imports definitions for all the available GekkoFS
## features
################################################################################
include(gkfs-options)

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)


# ##############################################################################
# Check for and/or download dependencies:
#
# GekkoFS has two types of dependencies:
# 1. Dependencies that are installed as system-wide packages and provide
#    a FindXXX.cmake file to find them. These are handled using
#    `find_package()`.
# 2. Depedencies that are used directly from source code (e.g. header-only
#    libraries). The source code for these can be found locally or
#    remotely in a GIT repository. These are handled using our custom
#    `include_from_source()` function defined in `gkfs-utils.cmake`
# ##############################################################################

# Search for dependencies that should already be installed in the system
###############################################################################

### some dependencies don't provide CMake modules, but rely on pkg-config
### instead, make sure that pkg-config is available
find_package(PkgConfig REQUIRED)

### Mercury
message(STATUS "[${PROJECT_NAME}] Checking for Mercury")
find_package(Mercury 2.0.1 REQUIRED)

### Argobots
message(STATUS "[${PROJECT_NAME}] Checking for Argobots")
find_package(Argobots 1.1 REQUIRED)

### Margo
message(STATUS "[${PROJECT_NAME}] Checking for Margo")
find_package(Margo 0.9.6 REQUIRED)

### syscall-intercept
message(STATUS "[${PROJECT_NAME}] Checking for syscall_intercept")
find_package(Syscall_intercept REQUIRED)

### Date (required by the libgkfs_intercept.so)
message(STATUS "[${PROJECT_NAME}] Checking for Date")
find_package(Date REQUIRED)

### AGIOS: required for scheduling I/O requests
if (GKFS_ENABLE_AGIOS)
    message(STATUS "[${PROJECT_NAME}] Checking for Agios")
    find_package(AGIOS REQUIRED)
endif()

### Metadata backends
if(GKFS_ENABLE_ROCKSDB)
    message(STATUS "[${PROJECT_NAME}] Checking for RocksDB")
    add_compile_definitions(GKFS_ENABLE_ROCKSDB)
    find_package(RocksDB REQUIRED)
    message(STATUS "[${PROJECT_NAME}] RocksDB version ${RocksDB_VERSION}")
    message(STATUS "[${PROJECT_NAME}] RocksDB location ${RocksDB_DIR}")
endif()

if(GKFS_ENABLE_PARALLAX)
    add_compile_definitions(GKFS_ENABLE_PARALLAX)
    # The current version of Parallax has a direct dependency on libyaml and librt.
    # Make sure that we find them and add them as link dependencies..

    message(STATUS "[${PROJECT_NAME}] Checking for yaml")
    find_package(yaml REQUIRED)

    message(STATUS "[${PROJECT_NAME}] Checking for libaio")
    find_package(AIO REQUIRED)

    message(STATUS "[${PROJECT_NAME}] Checking for Parallax")
    find_package(Parallax REQUIRED)
    target_link_libraries(Parallax::parallax INTERFACE yaml AIO::AIO)
endif()

### Prometheus-cpp: required for the collection of GekkoFS stats
### (these expose the prometheus-cpp::pull, prometheus-cpp::push,
### prometheus-cpp::core, and curl imported targets
if(GKFS_ENABLE_PROMETHEUS)
    find_package(CURL 7.68.0 REQUIRED)
    find_package(prometheus-cpp REQUIRED) # >= 1.0.0
endif()

### Other stuff that can be found out using find_package:

# determine the thread library of the system
find_package(Threads REQUIRED)

# some compilers need extra flags for std::filesystem, such as -lstdc++fs, this
# produces a std::filesystem imported target that takes care of all these
# details transparently
find_package(Filesystem REQUIRED)


# Search for 'source-only' dependencies
###############################################################################

# the directory where external dependencies should be searched for
set(GKFS_DEPENDENCIES_PATH ${CMAKE_SOURCE_DIR}/external)

### {fmt}: required for sensible output formatting
include_from_source(fmt
  MESSAGE "[${PROJECT_NAME}] Searching for {fmt}"
  SOURCE_DIR ${GKFS_DEPENDENCIES_PATH}/fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt
  GIT_TAG f94b7364b9409f05207c3af3fa4666730e11a854 # v6.1.2
)

# ensure that fmt is linked as PIC
set_property(TARGET fmt PROPERTY POSITION_INDEPENDENT_CODE ON)

### spdlog: required for logging
include_from_source(spdlog
  MESSAGE "[${PROJECT_NAME}] Searching for spdlog"
  SOURCE_DIR ${GKFS_DEPENDENCIES_PATH}/spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG eb3220622e73a4889eee355ffa37972b3cac3df5 # v1.9.2
  )

### CLI11: used for parsing command-line options
include_from_source(cli11
  MESSAGE "[${PROJECT_NAME}] Searching for CLI11"
  SOURCE_DIR ${GKFS_DEPENDENCIES_PATH}/CLI11
  GIT_REPOSITORY https://github.com/CLIUtils/CLI11
  GIT_TAG v2.2.0
  )


################################################################################
## Check configured variables/options and act accordingly
################################################################################
if(GKFS_BUILD_DOCUMENTATION)
    add_subdirectory(docs)
endif()

if (GKFS_SYMLINK_SUPPORT)
    add_definitions(-DHAS_SYMLINKS)
endif ()

if (GKFS_RENAME_SUPPORT)
    # Rename depends on symlink support
    add_definitions(-DHAS_SYMLINKS)
    set(GKFS_SYMLINK_SUPPORT ON)
    add_definitions(-DHAS_RENAME)
endif ()

if(GKFS_MAX_INTERNAL_FDS)
    add_definitions(-DGKFS_MAX_INTERNAL_FDS=${GKFS_MAX_INTERNAL_FDS})
endif()

if(GKFS_MAX_OPEN_FDS)
    add_definitions(-DGKFS_MAX_OPEN_FDS=${GKFS_MAX_OPEN_FDS})
endif()

if(GKFS_ENABLE_CLIENT_LOG)
    add_definitions(-DGKFS_ENABLE_LOGGING)
    add_definitions(-DLIBGKFS_LOG_MESSAGE_SIZE=${GKFS_CLIENT_LOG_MESSAGE_SIZE})
endif ()

if(GKFS_ENABLE_UNUSED_FUNCTIONS)
    add_definitions(-DGKFS_ENABLE_UNUSED_FUNCTIONS)
endif ()

if(GKFS_ENABLE_PROMETHEUS)
    add_definitions(-DGKFS_ENABLE_PROMETHEUS)
endif ()

configure_file(include/common/cmake_configure.hpp.in include/common/cmake_configure.hpp)

if(GKFS_ENABLE_CLIENT_LOG)
    option(HERMES_LOGGING "" ON)
    option(HERMES_LOGGING_FMT_USE_BUNDLED "" OFF)
    option(HERMES_LOGGING_FMT_HEADER_ONLY "" OFF)
endif ()

option(HERMES_MARGO_COMPATIBLE_RPCS "" ON)
add_subdirectory(external/hermes)
target_include_directories(hermes INTERFACE external/hermes/include)

set(INCLUDE_DIR "${CMAKE_SOURCE_DIR}/include")

# define include directories that are relevant for all targets
include_directories(
    ${INCLUDE_DIR}
    ${CMAKE_BINARY_DIR}/include
)

add_subdirectory(src)
add_subdirectory(include)

### Mark any CMake variables imported from {fmt} and spdlog as advanced, so
### that they don't appear in cmake-gui or ccmake. Similarly for FETCHCONTENT
### variables.
mark_variables_as_advanced(REGEX "^(FETCHCONTENT|fmt|FMT|spdlog|SPDLOG)_.*$")

if (GKFS_BUILD_TESTS)
    # Boost preprocessor header-only is supplied by the Mercury installation
    find_package(Boost_preprocessor REQUIRED)
    # check symbols exists doesn't work for statx. This is a workaround
    check_cxx_source_compiles("
        #include <fcntl.h>
        #include <sys/stat.h>

        int main() {
            struct statx buf;
            statx(AT_FDCWD, \"/foo\", AT_EMPTY_PATH, STATX_BASIC_STATS, &buf);
            return 0;
        }
        " GLIBC_HAS_STATX)
    # STATX_TYPE must be set for c++17 for statx() to be found for tests
    if (GLIBC_HAS_STATX)
        add_definitions(-DSTATX_TYPE=1)
    endif ()
    message(STATUS "[gekkofs] Preparing tests...")
    set(GKFS_TESTS_INTERFACE "lo" CACHE STRING "Network interface to use when running tests (default: lo)")
    message(STATUS "[gekkofs] Network interface for tests: ${GKFS_TESTS_INTERFACE}")

    message(STATUS "[gekkofs] Check for forwarding tests...")
    if (GKFS_ENABLE_FORWARDING)
        set(GKFS_TESTS_FORWARDING "ON" CACHE STRING "Enable I/O forwarding tests (default: OFF)")
    else ()
        set(GKFS_TESTS_FORWARDING "OFF" CACHE STRING "Enable I/O forwarding tests (default: OFF)")
    endif ()
    message(STATUS "[gekkofs] Forwarding tests: ${GKFS_TESTS_FORWARDING}")
    message(STATUS "[gekkofs] Check for guided distributor tests...")
    if (GKFS_USE_GUIDED_DISTRIBUTION)
        set(GKFS_TESTS_GUIDED_DISTRIBUTION "ON" CACHE STRING "Enable guided distributor tests (default: OFF)")
    else()
        set(GKFS_TESTS_GUIDED_DISTRIBUTION "OFF" CACHE STRING "Enable guided distributor tests (default: OFF)")
    endif()
    message(STATUS "[gekkofs] Guided distributor tests: ${GKFS_TESTS_GUIDED_DISTRIBUTION}")

    add_subdirectory(tests)
    add_subdirectory(examples/gfind)
else()
    unset(GKFS_TESTS_INTERFACE CACHE)
endif()

################################################################################
## Print GekkoFS configuration summary
################################################################################
gkfs_feature_summary(DESCRIPTION "${PROJECT_NAME} configuration summary:")
