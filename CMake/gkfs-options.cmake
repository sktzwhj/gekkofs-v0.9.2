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

function(_get_feature_summary _output _property)

  set(_currentFeatureText "")
  get_property(_features GLOBAL PROPERTY ${_property})

  if(_features)
    list(REMOVE_DUPLICATES _features)
  endif()

  foreach(_f ${_features})
    string(APPEND _currentFeatureText "\n-- * ${_f}")
    get_property(_info GLOBAL PROPERTY _CMAKE_${_f}_DESCRIPTION)
    if(_info)
      string(APPEND _currentFeatureText ": ${_info}")
    else()
      get_property(_info GLOBAL PROPERTY _CMAKE_${_f}_HELP_TEXT)
      string(APPEND _currentFeatureText ": ${_info}")
    endif()
    get_property(_info GLOBAL PROPERTY _CMAKE_${_f}_DEPENDS)
    if(_info)
      string(APPEND _currentFeatureText " [${${_info}}]")
    endif()
    get_property(_info GLOBAL PROPERTY _CMAKE_${_f}_EXTRA_INFO)
    if(_info)
      string(APPEND _currentFeatureText "\n--   ${_info}")
    endif()

  endforeach()

  set(${_output}
      "${_currentFeatureText}"
      PARENT_SCOPE
  )

endfunction()

function(_add_feature_info _name _depends _help)
  set(_enabled 1)
  foreach(_d ${_depends})
    string(REGEX REPLACE " +" ";" _d "${_d}")
    if(${_d})

    else()
      set(_enabled 0)
      break()
    endif()
  endforeach()
  if(${_enabled})
    set_property(GLOBAL APPEND PROPERTY GKFS_ENABLED_FEATURES "${_name}")
  else()
    set_property(GLOBAL APPEND PROPERTY GKFS_DISABLED_FEATURES "${_name}")
  endif()

  set_property(GLOBAL PROPERTY _CMAKE_${_name}_DEPENDS ${_depends})
  set_property(GLOBAL PROPERTY _CMAKE_${_name}_HELP_TEXT "${_help}")

  if(ARGC GREATER_EQUAL 4)
    set_property(GLOBAL PROPERTY _CMAKE_${_name}_DESCRIPTION "${ARGV3}")
    if(ARGC EQUAL 5)
      set_property(GLOBAL PROPERTY _CMAKE_${_name}_EXTRA_INFO "${ARGV4}")
    endif()
  endif()
endfunction()

function(gkfs_feature_summary)

  set(OPTIONS)
  set(SINGLE_VALUE DESCRIPTION)
  set(MULTI_VALUE)

  cmake_parse_arguments(
    ARGS "${OPTIONS}" "${SINGLE_VALUE}" "${MULTI_VALUE}" ${ARGN}
  )

  if(ARGS_UNPARSED_ARGUMENTS)
    message(
      FATAL_ERROR
        "Unknown keywords given to gkfs_feature_summary(): \"${ARGS_UNPARSED_ARGUMENTS}\""
    )
  endif()

  if(NOT ARGS_DESCRIPTION)
    message(
      FATAL_ERROR
        "Missing mandatory keyword DESCRIPTION for gkfs_feature_summary()"
    )
  endif()

  list(APPEND _what GKFS_ENABLED_FEATURES GKFS_DISABLED_FEATURES)

  set(_text "\n-- ")
  string(APPEND _text "\n-- ====================================")
  string(APPEND _text "\n-- ${ARGS_DESCRIPTION}")
  string(APPEND _text "\n-- ====================================")
  foreach(_w ${_what})
    set(_tmp)
    _get_feature_summary(_tmp ${_w})

    if(_tmp)
      if(_text)
        string(APPEND _text "\n-- ${_tmp}")
      endif()
    endif()
  endforeach()

  string(APPEND _text "\n-- ")

  message(STATUS "${_text}")
endfunction()

function(gkfs_define_option varName)

  set(OPTIONS)
  set(SINGLE_VALUE HELP_TEXT DEFAULT_VALUE FEATURE_NAME DESCRIPTION EXTRA_INFO)
  set(MULTI_VALUE)

  cmake_parse_arguments(
    ARGS "${OPTIONS}" "${SINGLE_VALUE}" "${MULTI_VALUE}" ${ARGN}
  )

  if(ARGS_UNPARSED_ARGUMENTS)
    message(
      FATAL_ERROR
        "Unknown keywords given to gkfs_define_option(): \"${ARGS_UNPARSED_ARGUMENTS}\""
    )
  endif()

  if(NOT ARGS_HELP_TEXT)
    message(
      FATAL_ERROR "Missing mandatory keyword HELP_TEXT for gkfs_define_option()"
    )
  endif()

  if(NOT ARGS_FEATURE_NAME)
    set(ARGS_FEATURE_NAME ${varName})
  endif()

  _add_feature_info(
    ${ARGS_FEATURE_NAME} ${varName} ${ARGS_HELP_TEXT} ${ARGS_DESCRIPTION}
    ${ARGS_EXTRA_INFO}
  )

  option(${varName} ${ARGS_HELP_TEXT} ${ARGS_DEFAULT_VALUE})

endfunction()

function(gkfs_define_variable varName value type docstring)

  set(OPTIONS ADVANCED)
  set(SINGLE_VALUE)
  set(MULTI_VALUE)

  cmake_parse_arguments(
    ARGS "${OPTIONS}" "${SINGLE_VALUE}" "${MULTI_VALUE}" ${ARGN}
  )

  if(ARGS_UNPARSED_ARGUMENTS)
    message(
      FATAL_ERROR
      "Unknown keywords given to gkfs_define_variable(): \"${ARGS_UNPARSED_ARGUMENTS}\""
    )
  endif()

  set(${varName} ${value} CACHE ${type} ${docstring})
  _add_feature_info(${varName} ${varName} ${docstring})

  if(ARGS_ADVANCED)
    mark_as_advanced(varName)
  endif()

endfunction()


################################################################################
# Variables and options controlling the build process
################################################################################

# build documentation
gkfs_define_option(
  GKFS_BUILD_DOCUMENTATION
  HELP_TEXT "Build documentation"
  DEFAULT_VALUE OFF
  DESCRIPTION "Generate documentation based on Sphinx+Doxygen"
  FEATURE_NAME "DOCS"
)

# build tests
gkfs_define_option(
    GKFS_ENABLE_UNUSED_FUNCTIONS
    HELP_TEXT "Enable unused functions compilation"
    DEFAULT_VALUE OFF
)


# build tests
gkfs_define_option(
  GKFS_BUILD_TESTS
  HELP_TEXT "Build ${PROJECT_NAME} self tests"
  DEFAULT_VALUE OFF
)

cmake_dependent_option(GKFS_INSTALL_TESTS "Install GekkoFS self tests" OFF "GKFS_BUILD_TESTS" OFF)


################################################################################
# Variables and options controlling POSIX semantics
################################################################################

## check before create
gkfs_define_option(
  GKFS_CREATE_CHECK_PARENTS
  HELP_TEXT "Enable checking parent directory for existence before creating children"
  DEFAULT_VALUE ON
  DESCRIPTION "Verify that a parent directory exists before creating new files or directories"
)

## symbolic link support
gkfs_define_option(
  GKFS_SYMLINK_SUPPORT
  HELP_TEXT "Enable support for symlinks"
  DEFAULT_VALUE ON
  DESCRIPTION "Enable support for symbolic links in paths"
)

## rename support
gkfs_define_option(
  GKFS_RENAME_SUPPORT
  HELP_TEXT "Enable support for rename"
  DEFAULT_VALUE OFF
  DESCRIPTION "Compile with support for rename ops (experimental)"
)


################################################################################
# Options and variables that control how GekkoFS behaves internally
################################################################################

## Maximum number of internal file descriptors reserved for GekkoFS
gkfs_define_variable(GKFS_MAX_INTERNAL_FDS 256
  STRING "Number of file descriptors reserved for internal use" ADVANCED
)

## Maximum number of open file descriptors for GekkoFS
execute_process(COMMAND getconf OPEN_MAX
  OUTPUT_VARIABLE _GETCONF_MAX_FDS
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET)
if (NOT _GETCONF_MAX_FDS)
  set(_GETCONF_MAX_FDS=512)
endif ()

gkfs_define_variable(
  GKFS_MAX_OPEN_FDS
  ${_GETCONF_MAX_FDS}
  STRING
  "Maximum number of open file descriptors supported"
  ADVANCED)

## RocksDB support
gkfs_define_option(
  GKFS_ENABLE_ROCKSDB
  HELP_TEXT "Enable RocksDB metadata backend"
  DEFAULT_VALUE ON
  DESCRIPTION "Use RocksDB key-value store for the metadata backend"
)

## Parallax support
gkfs_define_option(
  GKFS_ENABLE_PARALLAX
  HELP_TEXT "Enable Parallax support"
  DEFAULT_VALUE OFF
  DESCRIPTION "Support using the Parallax key-value store in the metadata backend"
)

## Guided distribution
gkfs_define_variable(
  GKFS_USE_GUIDED_DISTRIBUTION_PATH
  "/tmp/guided.txt"
  STRING
  "File Path for guided distributor"
)

gkfs_define_option(
  GKFS_USE_GUIDED_DISTRIBUTION
  HELP_TEXT "Use guided data distributor"
  DEFAULT_VALUE OFF
  DESCRIPTION "Use a guided data distribution instead of GekkoFS' wide striping"
  EXTRA_INFO "Guided data distributor input file path: ${GKFS_USE_GUIDED_DISTRIBUTION_PATH}"
)


################################################################################
# Logging and tracing support
################################################################################

## Client logging support
gkfs_define_option(
  GKFS_ENABLE_CLIENT_LOG HELP_TEXT "Enable logging messages in clients"
  DEFAULT_VALUE ON
)

gkfs_define_variable(
  GKFS_CLIENT_LOG_MESSAGE_SIZE
  1024
  STRING
  "Maximum size of a log message in the client library"
  ADVANCED
)

## Prometheus tracing support
gkfs_define_option(
  GKFS_ENABLE_PROMETHEUS
  HELP_TEXT "Enable Prometheus Push"
  DEFAULT_VALUE OFF
  DESCRIPTION "Enable the collection of stats using Prometheus"
)


################################################################################
# I/O forwarding
################################################################################

## Forwarding support
gkfs_define_option(
  GKFS_ENABLE_FORWARDING
  HELP_TEXT "Enable I/O forwarding mode"
  DEFAULT_VALUE OFF
  DESCRIPTION "Use ${PROJECT_NAME} as an I/O forwarding layer"
)

## Scheduling in I/O forwarding mode
gkfs_define_option(
  GKFS_ENABLE_AGIOS
  HELP_TEXT "Enable AGIOS scheduling library"
  DEFAULT_VALUE OFF
  DESCRIPTION "If GKFS_ENABLE_FORWARDING is ON, use AGIOS for scheduling I/Os"
)


