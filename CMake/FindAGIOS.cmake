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
  AGIOS_INCLUDE_DIR
  NAMES agios.h
  PATH_SUFFIXES include
)

find_library(AGIOS_LIBRARY NAMES agios)

set(AGIOS_INCLUDE_DIRS ${AGIOS_INCLUDE_DIR})
set(AGIOS_LIBRARIES ${AGIOS_LIBRARY})

mark_as_advanced(AGIOS_LIBRARY AGIOS_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  AGIOS
  FOUND_VAR AGIOS_FOUND
  REQUIRED_VARS AGIOS_LIBRARY AGIOS_INCLUDE_DIR
)

if(AGIOS_FOUND)
  set(AGIOS_INCLUDE_DIRS ${AGIOS_INCLUDE_DIR})
  set(AGIOS_LIBRARIES ${AGIOS_LIBRARY})
  if(NOT TARGET AGIOS::AGIOS)
    add_library(AGIOS::AGIOS UNKNOWN IMPORTED)
    set_target_properties(
      AGIOS::AGIOS
      PROPERTIES IMPORTED_LOCATION "${AGIOS_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${AGIOS_INCLUDE_DIR}"
    )
  endif()
endif( )

