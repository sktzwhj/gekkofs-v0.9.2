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

find_package(Doxygen REQUIRED dot)

set(DOXYGEN_FULL_PATH_NAMES YES)
set(DOXYGEN_WARN_IF_UNDOCUMENTED NO)

# Sphinx needs the Doxygen's XML output, but doesn't care about its HTML output
set(DOXYGEN_GENERATE_HTML NO)
set(DOXYGEN_GENERATE_XML YES)

set(DOXYGEN_GENERATE_TREEVIEW NO)
set(DOXYGEN_GENERATE_TODOLIST YES)
set(DOXYGEN_GENERATE_BUGLIST YES)
set(DOXYGEN_GENERATE_HTMLHELP NO)
set(DOXYGEN_GENERATE_LATEX NO)
set(DOXYGEN_GENERATE_MAN NO)
set(DOXYGEN_GENERATE_RTF NO)

set(DOXYGEN_ENABLE_PREPROCESSING YES)
set(DOXYGEN_MACRO_EXPANSION YES)

set(DOXYGEN_SEARCH_INCLUDES YES)
set(DOXYGEN_EXPAND_ONLY_PREDEF YES)

# extra defs for exhale to help with building the _right_ version of the docs
list(APPEND DOXYGEN_PREDEFINED
  DOXYGEN_DOCUMENTATION_BUILD
  DOXYGEN_SHOULD_SKIP_THIS)
set(DOXYGEN_CREATE_SUBDIRS NO)

# Allow for rst directives and advanced functions e.g. grid tables
list(APPEND DOXYGEN_ALIASES
  "\"rst=\\verbatim embed:rst:leading-asterisk\""
  "\"endrst=\\endverbatim\"")

set(DOXYGEN_ENABLE_PREPROCESSING YES)
set(DOXYGEN_MACRO_EXPANSION YES)
set(DOXYGEN_EXPAND_ONLY_PREDEF NO)

set(DOXYGEN_STRIP_FROM_PATH ${CMAKE_SOURCE_DIR})

list(APPEND
  DOXYGEN_EXCLUDE_PATTERNS
  "${PROJECT_SOURCE_DIR}/*/rpc_types.*"
  "${PROJECT_SOURCE_DIR}/*/detail/*"
  "${PROJECT_SOURCE_DIR}/tests/*/test_*.cpp"
  "${PROJECT_SOURCE_DIR}/tests/integration/*"
  "${PROJECT_SOURCE_DIR}/*/README.md")

doxygen_add_docs(
  doxygen_docs
  ${PROJECT_SOURCE_DIR}/src
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_SOURCE_DIR}/tests
  COMMENT "Generating Doxygen documentation"
  )

## ensure Doxygen files are cleaned with 'make clean'
## (this should be done by doxygen_add_docs() but...)
list(APPEND
  _doxygen_generated_files
  ${CMAKE_CURRENT_BINARY_DIR}/xml
  )
set_target_properties(doxygen_docs
  PROPERTIES ADDITIONAL_CLEAN_FILES "${_doxygen_generated_files}")
