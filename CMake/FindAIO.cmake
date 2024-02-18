
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
find_path(
  AIO_INCLUDE_DIR
  NAMES aio.h
  PATH_SUFFIXES include
)

find_library(AIO_LIBRARY NAMES rt)

mark_as_advanced(AIO_INCLUDE_DIR AIO_LIBRARY)

find_package_handle_standard_args(
  AIO
  FOUND_VAR AIO_FOUND
  REQUIRED_VARS AIO_INCLUDE_DIR AIO_LIBRARY
)

if(AIO_FOUND AND NOT TARGET AIO::AIO)
  add_library(AIO::AIO UNKNOWN IMPORTED)
  if(AIO_INCLUDE_DIR)
    set_target_properties(
      AIO::AIO PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${AIO_INCLUDE_DIR}"
    )
  endif()

  set_target_properties(
    AIO::AIO PROPERTIES IMPORTED_LOCATION "${AIO_LIBRARY}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
  )
endif()

