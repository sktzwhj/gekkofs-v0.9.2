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

include(CMakeParseArguments)

#[=======================================================================[.rst:

  get_cmake_variables(OUTPUT_VARIABLE
                     [ REGEX <regular_expression> [EXCLUDE] ])

Initialize ``OUTPUT_VARIABLE`` to a list of all currently defined CMake
variables. The function accepts a ``<regular_expression>`` to allow filtering
the results. Furthermore, if the ``EXCLUDE`` flag is used, the function will
return all variables NOT MATCHING the provided ``<regular_expression>``.
#]=======================================================================]
function(get_cmake_variables OUTPUT_VARIABLE)

  set(OPTIONS EXCLUDE)
  set(SINGLE_VALUE REGEX)
  set(MULTI_VALUE) # no multiple value args for now

  cmake_parse_arguments(
    ARGS "${OPTIONS}" "${SINGLE_VALUE}" "${MULTI_VALUE}" ${ARGN}
  )

  if(ARGS_UNPARSED_ARGUMENTS)
    message(WARNING "Unparsed arguments in get_cmake_variables(): "
                    "this often indicates typos!\n"
                    "Unparsed arguments: ${ARGS_UNPARSED_ARGUMENTS}"
    )
  endif()

  get_cmake_property(_var_names VARIABLES)

  if(NOT ARGS_REGEX)
    set(${OUTPUT_VARIABLE}
        ${_var_names}
        PARENT_SCOPE
    )
    return()
  endif()

  if(ARGS_EXCLUDE)
    set(_mode EXCLUDE)
  else()
    set(_mode INCLUDE)
  endif()

  list(FILTER _var_names ${_mode} REGEX ${ARGS_REGEX})
  set(${OUTPUT_VARIABLE}
      ${_var_names}
      PARENT_SCOPE
  )
endfunction()

#[=======================================================================[.rst:

  dump_cmake_variables([ REGEX <regular_expression> [EXCLUDE] ])

Print all currently defined CMake variables. The function accepts a
``<regular_expression>`` to allow filtering the results. Furthermore, if the
``EXCLUDE`` flag is used, the function will print all variables NOT MATCHING
the provided ``<regular_expression>``.
#]=======================================================================]
function(dump_cmake_variables)

  set(OPTIONS EXCLUDE)
  set(SINGLE_VALUE REGEX)
  set(MULTI_VALUE) # no multiple value args for now

  cmake_parse_arguments(
    ARGS "${OPTIONS}" "${SINGLE_VALUE}" "${MULTI_VALUE}" ${ARGN}
  )

  if(ARGS_UNPARSED_ARGUMENTS)
    message(WARNING "Unparsed arguments in dump_cmake_variables(): "
                    "this often indicates typos!"
                    "Unparsed arguments: ${ARGS_UNPARSED_ARGUMENTS}"
    )
  endif()

  if(ARGS_EXCLUDE AND NOT ARGS_REGEX)
    message(ERROR "EXCLUDE option doesn't make sense without REGEX.")
  endif()

  get_cmake_variables(_var_names REGEX ${ARGS_REGEX} ${ARGS_EXCLUDE})

  foreach(_var ${_var_names})
    message(STATUS "${_var}=${${_var}}")
  endforeach()
endfunction()

#[=======================================================================[.rst:

  mark_variables_as_advanced(REGEX <regular_expression>)

Mark all CMake variables matching ``regular_expression`` as advanced.
#]=======================================================================]
function(mark_variables_as_advanced)

  set(OPTIONS) # no options for now
  set(SINGLE_VALUE REGEX)
  set(MULTI_VALUE) # no multiple value args for now

  cmake_parse_arguments(
    ARGS "${OPTIONS}" "${SINGLE_VALUE}" "${MULTI_VALUE}" ${ARGN}
  )

  if(ARGS_UNPARSED_ARGUMENTS)
    message(WARNING "Unparsed arguments in mark_variables_as_advanced(): "
                    "this often indicates typos!\n"
                    "Unparsed arguments: ${ARGS_UNPARSED_ARGUMENTS}"
    )
  endif()

  get_cmake_property(_var_names VARIABLES)

  list(FILTER _var_names INCLUDE REGEX ${ARGS_REGEX})

  foreach(_var ${_var_names})
    mark_as_advanced(${_var})
  endforeach()
endfunction()


#[=======================================================================[.rst:

  include_from_source(contentName <options>...)

The ``include_from_source()`` function ensures that ``contentName`` is
populated and potentially added to the build by the time it returns.

**Options:**

  ``SOURCE_DIR <dir>``: Source directory into which downloaded contents reside.
    This must point to an existing directory where the external project has
    already been unpacked or cloned/checked out. If ``<dir>`` doesn't exist,
    the source code will be retrieved.

  ``GIT_REPOSITORY <url>``
    URL of the git repository. Any URL understood by the ``git`` command
    may be used.

  ``GIT_TAG <tag>``
    Git branch name, tag or commit hash. Note that branch names and tags should
    generally be specified as remote names (i.e. origin/myBranch rather than
    simply myBranch). This ensures that if the remote end has its tag moved or
    branch rebased or history rewritten, the local clone will still be updated
    correctly. In general, however, specifying a commit hash should be
    preferred for a number of reasons:

    If the local clone already has the commit corresponding to the hash, no git
    fetch needs to be performed to check for changes each time CMake is re-run.
    This can result in a significant speed up if many external projects are
    being used.

    Using a specific git hash ensures that the main project's own history is
    fully traceable to a specific point in the external project's evolution.
    If a branch or tag name is used instead, then checking out a specific
    commit of the main project doesn't necessarily pin the whole build to a
    specific point in the life of the external project. The lack of such
    deterministic behavior makes the main project lose traceability and
    repeatability.

  NOTE: If both ``SOURCE_DIR`` and ``GIT_REPOSITORY`` are specified,
  ``SOURCE_DIR`` will be the preferred location to populate ``contentName``
  from. If ``SOURCE_DIR`` doesn't exist, the function will fall back to the
  location defined by ``GIT_REPOSITORY``.

#]=======================================================================]
function(include_from_source contentName)

  set(OPTIONS)
  set(SINGLE_VALUE MESSAGE SOURCE_DIR GIT_REPOSITORY GIT_TAG)
  set(MULTI_VALUE)

  cmake_parse_arguments(ARGS "${OPTIONS}" "${SINGLE_VALUE}" "${MULTI_VALUE}" ${ARGN})

  if(ARGS_MESSAGE)
    message(STATUS ${ARGS_MESSAGE})
  endif()

  include(FetchContent)

  if (EXISTS ${ARGS_SOURCE_DIR})
    file(GLOB_RECURSE SOURCE_FILES "${ARGS_SOURCE_DIR}/*")
    if(SOURCE_FILES STREQUAL "")
      message(FATAL_ERROR
        "The '${ARGS_SOURCE_DIR}' source directory appears "
        "to be empty. If it corresponds to a git submodule it may not have "
        "been properly initialized. Running:\n"
        "  'git submodule update --init --recursive'\n"
        "may fix the issue. If the directory corresponds to a manually "
        "downloaded dependency, please download it again.")
    endif()

    message(STATUS "Found source directory for '${contentName}'. Building.")
    FetchContent_Declare(
      ${contentName}
      SOURCE_DIR ${ARGS_SOURCE_DIR}
    )
  else()
    message(STATUS
      "Source directory for '${contentName}' not found.\n"
      "Downloading and building from remote Git repository.")

    if(NOT ARGS_GIT_REPOSITORY)
      message(FATAL_ERROR "GIT_REPOSITORY for \"${contentName}\" not defined")
    endif()

    if(NOT ARGS_GIT_TAG)
      message(FATAL_ERROR "GIT_TAG for \"${contentName}\" not defined")
    endif()

    FetchContent_Declare(
      ${contentName}
      GIT_REPOSITORY ${ARGS_GIT_REPOSITORY}
      GIT_TAG ${ARGS_GIT_TAG}
      GIT_SHALLOW ON
      GIT_PROGRESS ON
    )
  endif()

  FetchContent_MakeAvailable(${contentName})
endfunction()
