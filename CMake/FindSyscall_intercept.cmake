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

find_package(PkgConfig)
pkg_check_modules(PC_Syscall_intercept QUIET libsyscall_intercept)

find_path(Syscall_intercept_INCLUDE_DIR
  NAMES libsyscall_intercept_hook_point.h
  PATHS ${PC_Syscall_intercept_INCLUDE_DIRS}
)

find_library(Syscall_intercept_LIBRARY
  NAMES syscall_intercept
  PATHS ${PC_Syscall_intercept_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
	Syscall_intercept
	DEFAULT_MSG
	Syscall_intercept_INCLUDE_DIR
	Syscall_intercept_LIBRARY
)

if(Syscall_intercept_FOUND)
  set(Syscall_intercept_LIBRARIES ${Syscall_intercept_LIBRARY})
  set(Syscall_intercept_INCLUDE_DIRS ${Syscall_intercept_INCLUDE_DIR})
  set(Syscall_intercept_DEFINITIONS ${PC_Syscall_intercept_CFLAGS_OTHER})

  if(NOT TARGET Syscall_intercept::Syscall_intercept)
	  add_library(Syscall_intercept::Syscall_intercept UNKNOWN IMPORTED)
	  set_target_properties(Syscall_intercept::Syscall_intercept PROPERTIES
		IMPORTED_LOCATION "${Syscall_intercept_LIBRARY}"
		INTERFACE_COMPILE_OPTIONS "${PC_Syscall_intercept_CFLAGS_OTHER}"
		INTERFACE_INCLUDE_DIRECTORIES "${Syscall_intercept_INCLUDE_DIR}"
	  )
	endif()
endif()


mark_as_advanced(
	Syscall_intercept_INCLUDE_DIR
	Syscall_intercept_LIBRARY
)
