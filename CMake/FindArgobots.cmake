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

#[=======================================================================[.rst:
FindArgobots
---------

Find Argobots include dirs and libraries.

Use this module by invoking find_package with the form::

  find_package(Argobots
    [version] [EXACT]     # Minimum or EXACT version e.g. 0.6.2
    [REQUIRED]            # Fail with error if Argobots is not found
    )

Imported Targets
^^^^^^^^^^^^^^^^

This module provides the following imported targets, if found:

``Argobots::Argobots``
  The Argobots library

Result Variables
^^^^^^^^^^^^^^^^

This will define the following variables:

``Argobots_FOUND``
  True if the system has the Argobots library.
``Argobots_VERSION``
  The version of the Argobots library which was found.
``Argobots_INCLUDE_DIRS``
  Include directories needed to use Argobots.
``Argobots_LIBRARIES``
  Libraries needed to link to Argobots.

Cache Variables
^^^^^^^^^^^^^^^

The following cache variables may also be set:

``ARGOBOTS_INCLUDE_DIR``
  The directory containing ``abt.h``.
``ARGOBOTS_LIBRARY``
  The path to the Argobots library.

#]=======================================================================]

function(_get_pkgconfig_paths target_var)
  set(_lib_dirs)
  if(NOT DEFINED CMAKE_SYSTEM_NAME
     OR (CMAKE_SYSTEM_NAME MATCHES "^(Linux|kFreeBSD|GNU)$"
         AND NOT CMAKE_CROSSCOMPILING)
  )
    if(EXISTS "/etc/debian_version") # is this a debian system ?
      if(CMAKE_LIBRARY_ARCHITECTURE)
        list(APPEND _lib_dirs "lib/${CMAKE_LIBRARY_ARCHITECTURE}/pkgconfig")
      endif()
    else()
      # not debian, check the FIND_LIBRARY_USE_LIB32_PATHS and FIND_LIBRARY_USE_LIB64_PATHS properties
      get_property(uselib32 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB32_PATHS)
      if(uselib32 AND CMAKE_SIZEOF_VOID_P EQUAL 4)
        list(APPEND _lib_dirs "lib32/pkgconfig")
      endif()
      get_property(uselib64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
      if(uselib64 AND CMAKE_SIZEOF_VOID_P EQUAL 8)
        list(APPEND _lib_dirs "lib64/pkgconfig")
      endif()
      get_property(uselibx32 GLOBAL PROPERTY FIND_LIBRARY_USE_LIBX32_PATHS)
      if(uselibx32 AND CMAKE_INTERNAL_PLATFORM_ABI STREQUAL "ELF X32")
        list(APPEND _lib_dirs "libx32/pkgconfig")
      endif()
    endif()
  endif()
  if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" AND NOT CMAKE_CROSSCOMPILING)
    list(APPEND _lib_dirs "libdata/pkgconfig")
  endif()
  list(APPEND _lib_dirs "lib/pkgconfig")
  list(APPEND _lib_dirs "share/pkgconfig")

  set(_extra_paths)
  list(APPEND _extra_paths ${CMAKE_PREFIX_PATH})
  list(APPEND _extra_paths ${CMAKE_FRAMEWORK_PATH})
  list(APPEND _extra_paths ${CMAKE_APPBUNDLE_PATH})

  # Check if directories exist and eventually append them to the
  # pkgconfig path list
  foreach(_prefix_dir ${_extra_paths})
    foreach(_lib_dir ${_lib_dirs})
      if(EXISTS "${_prefix_dir}/${_lib_dir}")
        list(APPEND _pkgconfig_paths "${_prefix_dir}/${_lib_dir}")
        list(REMOVE_DUPLICATES _pkgconfig_paths)
      endif()
    endforeach()
  endforeach()

  set("${target_var}"
      ${_pkgconfig_paths}
      PARENT_SCOPE
  )
endfunction()

# prevent repeating work if the main CMakeLists.txt already called
# find_package(PkgConfig)
if(NOT PKG_CONFIG_FOUND)
  find_package(PkgConfig)
endif()

if(PKG_CONFIG_FOUND)
  pkg_check_modules(PC_ARGOBOTS QUIET argobots)

  find_path(
    ARGOBOTS_INCLUDE_DIR
    NAMES abt.h
    PATHS ${PC_ARGOBOTS_INCLUDE_DIRS}
    PATH_SUFFIXES include
  )

  find_library(
    ARGOBOTS_LIBRARY
    NAMES abt
    PATHS ${PC_ARGOBOTS_LIBRARY_DIRS}
  )

  set(Argobots_VERSION ${PC_ARGOBOTS_VERSION})
else()
  find_path(
    ARGOBOTS_INCLUDE_DIR
    NAMES abt.h
    PATH_SUFFIXES include
  )

  find_library(ARGOBOTS_LIBRARY NAMES abt)

  # even if pkg-config is not available, but Argobots still installs a .pc file
  # that we can use to retrieve library information from. Try to find it at all
  # possible pkgconfig subfolders (depending on the system).
  _get_pkgconfig_paths(_pkgconfig_paths)

  find_file(_argobots_pc_file argobots.pc PATHS "${_pkgconfig_paths}")

  if(NOT _argobots_pc_file)
    message(
      FATAL_ERROR
        "ERROR: Could not find 'argobots.pc' file. Unable to determine library version"
    )
  endif()

  file(STRINGS "${_argobots_pc_file}" _argobots_pc_file_contents
       REGEX "Version: "
  )

  if("${_argobots_pc_file_contents}" MATCHES
     "Version: ([0-9]+\\.[0-9]+\\.[0-9])"
  )
    set(Argobots_VERSION ${CMAKE_MATCH_1})
  else()
    message(FATAL_ERROR "ERROR: Failed to determine library version")
  endif()

  unset(_pkg_config_paths)
  unset(_argobots_pc_file_contents)
endif()

mark_as_advanced(ARGOBOTS_INCLUDE_DIR ARGOBOTS_LIBRARY)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  Argobots
  FOUND_VAR Argobots_FOUND
  REQUIRED_VARS ARGOBOTS_LIBRARY ARGOBOTS_INCLUDE_DIR
  VERSION_VAR Argobots_VERSION
)

if(Argobots_FOUND)
  set(Argobots_INCLUDE_DIRS ${ARGOBOTS_INCLUDE_DIR})
  set(Argobots_LIBRARIES ${ARGOBOTS_LIBRARY})
  if(NOT TARGET Argobots::Argobots)
    add_library(Argobots::Argobots UNKNOWN IMPORTED)
    set_target_properties(
      Argobots::Argobots
      PROPERTIES IMPORTED_LOCATION "${ARGOBOTS_LIBRARY}"
                 INTERFACE_INCLUDE_DIRECTORIES "${ARGOBOTS_INCLUDE_DIR}"
    )
  endif()
endif()
