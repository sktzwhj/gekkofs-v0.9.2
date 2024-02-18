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

# ensure helper programs in the testing harness get built
add_subdirectory(harness)

# Create daemon factories from template, depending of the databases
unset(DBS)
if (GKFS_ENABLE_ROCKSDB)
    set (DBS "'gkfs_daemon_rocksdb'")
endif()
if (GKFS_ENABLE_PARALLAX)
    if (DBS)
        set (DBS "${DBS},'gkfs_daemon_parallaxdb'")
    else()
        set (DBS "'gkfs_daemon_parallaxdb'")
    endif()
endif()

if (GKFS_ENABLE_PARALLAX AND GKFS_ENABLE_ROCKSDB)
    set (DBS "'gkfs_daemon_rocksdb','gkfs_daemon_parallaxdb'")
endif()

# We enable rocksdb by default
if (NOT DBS)
        set (DBS "'gkfs_daemon_rocksdb'")
endif()

FIND_PATH(BUILD_PATH CMakeLists.txt . )
FILE(READ ${BUILD_PATH}/conftest.template CONF_TEST_FILE)
STRING(REGEX REPLACE "'gkfs_daemon_rocksdb'" "${DBS}" MOD_CONF_TEST_FILE "${CONF_TEST_FILE}" )
FILE(WRITE ${BUILD_PATH}/conftest.py "${MOD_CONF_TEST_FILE}")

gkfs_enable_python_testing(
    BINARY_DIRECTORIES ${CMAKE_BINARY_DIR}/src/daemon/
                       ${CMAKE_BINARY_DIR}/src/client/
                       ${CMAKE_BINARY_DIR}/tests/integration/harness/
                       ${CMAKE_BINARY_DIR}/examples/gfind/
    LIBRARY_PREFIX_DIRECTORIES ${CMAKE_PREFIX_PATH}
)

# define CTest tests for functional test groups
gkfs_add_python_test(
    NAME test_directories
    PYTHON_VERSION 3.6
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tests/integration
    SOURCE directories/
)

if (GLIBC_HAS_STATX)
    gkfs_add_python_test(
        NAME test_status
        PYTHON_VERSION 3.6
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tests/integration
        SOURCE status/test_status.py
    )
endif()

gkfs_add_python_test(
    NAME test_operations
    PYTHON_VERSION 3.6
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tests/integration
    SOURCE operations/
)

if (GLIBC_HAS_STATX)
gkfs_add_python_test(
    NAME test_lseek
    PYTHON_VERSION 3.6
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tests/integration
    SOURCE position/test_lseek.py
)
endif()

gkfs_add_python_test(
    NAME test_shell
    PYTHON_VERSION 3.6
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tests/integration
    SOURCE shell/
)

if (GKFS_TESTS_FORWARDING)
gkfs_add_python_test(
    NAME forwarding
    PYTHON_VERSION 3.6
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tests/integration
    SOURCE forwarding/
)
endif ()

gkfs_add_python_test(
    NAME test_data
    PYTHON_VERSION 3.6
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tests/integration
    SOURCE data/
)

gkfs_add_python_test(
    NAME test_syscalls
    PYTHON_VERSION 3.6
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tests/integration
    SOURCE syscalls/
)

if (GKFS_RENAME_SUPPORT)
gkfs_add_python_test(
    NAME test_rename
    PYTHON_VERSION 3.6
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tests/integration
    SOURCE rename/
)
endif()

if(GKFS_INSTALL_TESTS)
    install(DIRECTORY harness
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/integration
        FILES_MATCHING
            REGEX ".*\\.py"
            PATTERN "__pycache__" EXCLUDE
            PATTERN ".pytest_cache" EXCLUDE
            PATTERN "gkfs.io" EXCLUDE
    )

    if(GKFS_SYMLINK_SUPPORT)
        install(DIRECTORY directories
            DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/integration
            FILES_MATCHING
                REGEX ".*\\.py"
                PATTERN "__pycache__" EXCLUDE
                PATTERN ".pytest_cache" EXCLUDE
        )
    else()
        install(DIRECTORY directories
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/integration
        FILES_MATCHING
        REGEX ".*\\.py"
        PATTERN "__pycache__" EXCLUDE
        PATTERN ".pytest_cache" EXCLUDE
        PATTERN "test_symlink.py" EXCLUDE

    )
   endif()

    install(DIRECTORY status
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/integration
        FILES_MATCHING
            REGEX ".*\\.py"
            PATTERN "__pycache__" EXCLUDE
            PATTERN ".pytest_cache" EXCLUDE
    )

    install(DIRECTORY operations
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/integration
        FILES_MATCHING
            REGEX ".*\\.py"
            PATTERN "__pycache__" EXCLUDE
            PATTERN ".pytest_cache" EXCLUDE
    )

    install(DIRECTORY position
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/integration
        FILES_MATCHING
            REGEX ".*\\.py"
            PATTERN "__pycache__" EXCLUDE
            PATTERN ".pytest_cache" EXCLUDE
    )

    install(DIRECTORY data
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/integration
        FILES_MATCHING
            REGEX ".*\\.py"
            PATTERN "__pycache__" EXCLUDE
            PATTERN ".pytest_cache" EXCLUDE
    )

    install(DIRECTORY syscalls
    DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/integration
    FILES_MATCHING
        REGEX ".*\\.py"
        PATTERN "__pycache__" EXCLUDE
        PATTERN ".pytest_cache" EXCLUDE
)

    install(DIRECTORY shell
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/integration
        FILES_MATCHING
            REGEX ".*\\.py"
            PATTERN "__pycache__" EXCLUDE
            PATTERN ".pytest_cache" EXCLUDE
    )
    if (GKFS_TESTS_FORWARDING)
    install(DIRECTORY forwarding
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/integration
        FILES_MATCHING
            REGEX ".*\\.py"
            PATTERN "__pycache__" EXCLUDE
            PATTERN ".pytest_cache" EXCLUDE
    )
    endif ()

    if (GKFS_RENAME_SUPPORT)
    install(DIRECTORY rename
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/gkfs/tests/integration
        FILES_MATCHING
            REGEX ".*\\.py"
            PATTERN "__pycache__" EXCLUDE
            PATTERN ".pytest_cache" EXCLUDE
    )
    endif()
endif()



