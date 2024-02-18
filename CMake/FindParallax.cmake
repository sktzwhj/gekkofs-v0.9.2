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

list(APPEND _parallax_components "log")

find_path(
  PARALLAX_INCLUDE_DIR
  NAMES parallax.h
  PATH_SUFFIXES include
)

find_library(PARALLAX_LIBRARY NAMES parallax)

find_library(PARALLAX_LOG_LIBRARY NAMES log)

mark_as_advanced(PARALLAX_LIBRARY PARALLAX_INCLUDE_DIR PARALLAX_LOG_LIBRARY)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  Parallax
  FOUND_VAR Parallax_FOUND
  REQUIRED_VARS PARALLAX_LIBRARY PARALLAX_LOG_LIBRARY PARALLAX_INCLUDE_DIR
)

if(Parallax_FOUND)
  set(Parallax_INCLUDE_DIRS ${PARALLAX_INCLUDE_DIR})
  set(Parallax_LIBRARIES ${PARALLAX_LIBRARY} ${PARALLAX_LOG_LIBRARY})

  if(NOT TARGET Parallax::parallax)
    add_library(Parallax::parallax UNKNOWN IMPORTED)
  endif()

  set_target_properties(
    Parallax::parallax
    PROPERTIES IMPORTED_LOCATION "${PARALLAX_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${PARALLAX_INCLUDE_DIR}"
               IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
  )

  foreach(_component ${_parallax_components})
    if(NOT TARGET Parallax::${_component})
      add_library(Parallax::${_component} UNKNOWN IMPORTED)

      if(Parallax_INCLUDE_DIRS)
        set_target_properties(
          Parallax::${_component} PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                             "${PARALLAX_INCLUDE_DIR}"
        )

        set_target_properties(
          Parallax::${_component}
          PROPERTIES IMPORTED_LOCATION "${PARALLAX_LOG_LIBRARY}"
                     IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
        )
      endif()
    endif()
  endforeach()
endif()
