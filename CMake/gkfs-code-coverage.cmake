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

option(GKFS_GENERATE_COVERAGE_REPORTS "Generate coverage reports" ON)

macro(gkfs_enable_coverage_reports)

  set(OPTIONS)
  set(SINGLE_VALUE)
  set(MULTI_VALUE EXCLUDE_DIRECTORIES)
  cmake_parse_arguments(
    ARGS "${OPTIONS}" "${SINGLE_VALUE}" "${MULTI_VALUE}" ${ARGN}
  )

  find_program(COVERAGE_PY
    coverage.py
    PATHS ${CMAKE_SOURCE_DIR}/scripts/dev
    REQUIRED)

  if(NOT COVERAGE_OUTPUT_DIR)
    set(COVERAGE_OUTPUT_DIR "${CMAKE_BINARY_DIR}/coverage")
  endif()

  file(MAKE_DIRECTORY ${COVERAGE_OUTPUT_DIR})

  if(NOT COVERAGE_ZEROCOUNT_TRACEFILE)
    set(COVERAGE_ZEROCOUNT_TRACEFILE "${COVERAGE_OUTPUT_DIR}/zerocount.info")
  endif()

  if(NOT COVERAGE_CAPTURE_TRACEFILE)
    set(COVERAGE_CAPTURE_TRACEFILE "${COVERAGE_OUTPUT_DIR}/capture.info")
  endif()

  if(NOT COVERAGE_UNIFIED_TRACEFILE)
    set(COVERAGE_UNIFIED_TRACEFILE "${COVERAGE_OUTPUT_DIR}/coverage.info")
  endif()

  if(NOT COVERAGE_HTML_REPORT_DIRECTORY)
    set(COVERAGE_HTML_REPORT_DIRECTORY "${COVERAGE_OUTPUT_DIR}/coverage_html")
  endif()

  if(NOT COVERAGE_XML_REPORT)
    set(COVERAGE_XML_REPORT "${COVERAGE_OUTPUT_DIR}/coverage-cobertura.xml")
  endif()

  # add a `coverage-zerocount` target for the initial baseline gathering of
  # coverage information
  add_custom_command(
    OUTPUT "${COVERAGE_ZEROCOUNT_TRACEFILE}"
    COMMAND
      ${COVERAGE_PY}
        capture
        --initial
        --root-directory "${CMAKE_BINARY_DIR}"
        --output-file "${COVERAGE_ZEROCOUNT_TRACEFILE}"
        --sources-directory "${CMAKE_SOURCE_DIR}"
        "$<$<BOOL:${ARGS_EXCLUDE_DIRECTORIES}>:--exclude;$<JOIN:${ARGS_EXCLUDE_DIRECTORIES},;--exclude;>>"
    COMMAND_EXPAND_LISTS VERBATIM
    COMMENT "Generating zerocount coverage tracefile"
  )

  add_custom_target(coverage-zerocount
    DEPENDS ${COVERAGE_ZEROCOUNT_TRACEFILE})

  # add a `coverage-capture` target to capture coverage data from any
  # of the existing .gcda files
  add_custom_command(
    OUTPUT "${COVERAGE_CAPTURE_TRACEFILE}"
    COMMAND
    ${COVERAGE_PY}
    capture
    --root-directory "${CMAKE_BINARY_DIR}"
    --output-file "${COVERAGE_CAPTURE_TRACEFILE}"
    --sources-directory "${CMAKE_SOURCE_DIR}"
    "$<$<BOOL:${ARGS_EXCLUDE_DIRECTORIES}>:--exclude;$<JOIN:${ARGS_EXCLUDE_DIRECTORIES},;--exclude;>>"
    COMMAND_EXPAND_LISTS VERBATIM
    COMMENT "Generating capture coverage tracefile"
  )

  add_custom_target(coverage-capture DEPENDS ${COVERAGE_CAPTURE_TRACEFILE})

  # add a `coverage-unified` target to merge all coverage data available in
  # ${COVERAGE_OUTPUT_DIR} into a unified coverage trace
  add_custom_command(
    OUTPUT ${COVERAGE_UNIFIED_TRACEFILE}
    DEPENDS ${COVERAGE_CAPTURE_TRACEFILE}
    COMMAND
    ${COVERAGE_PY}
    merge
    --search-pattern "${COVERAGE_OUTPUT_DIR}/*.info"
    --output-file "${COVERAGE_UNIFIED_TRACEFILE}"
    VERBATIM
    COMMENT "Generating unified coverage tracefile"
  )

  add_custom_target(
    coverage-unified
    DEPENDS "${COVERAGE_UNIFIED_TRACEFILE}"
  )

  # add a `coverage-summary` target to print a summary of coverage data
  add_custom_target(
    coverage-summary
    COMMAND
    ${COVERAGE_PY}
    summary
    --input-tracefile ${COVERAGE_UNIFIED_TRACEFILE}
    DEPENDS ${COVERAGE_UNIFIED_TRACEFILE}
    COMMENT "Gathering coverage information"
  )

  # add a `coverage-html` target to generate a coverage HTML report
  add_custom_command(OUTPUT
    "${COVERAGE_HTML_REPORT_DIRECTORY}"
    COMMAND
    ${COVERAGE_PY}
    html_report
    --input-tracefile "${COVERAGE_UNIFIED_TRACEFILE}"
    --prefix "${CMAKE_SOURCE_DIR}"
    --output-directory "${COVERAGE_HTML_REPORT_DIRECTORY}"
    DEPENDS ${COVERAGE_UNIFIED_TRACEFILE}
    VERBATIM
    COMMENT "Generating HTML report"
    )

  add_custom_target(
    coverage-html
    DEPENDS "${COVERAGE_HTML_REPORT_DIRECTORY}")

  # add a `coverage-cobertura` target to generate a Cobertura XML report
  add_custom_command(OUTPUT
    "${COVERAGE_XML_REPORT}"
    COMMAND
    ${COVERAGE_PY}
    cobertura
    --input-tracefile "${COVERAGE_UNIFIED_TRACEFILE}"
    --base-dir "${CMAKE_SOURCE_DIR}"
    --output-file "${COVERAGE_XML_REPORT}"
    DEPENDS ${COVERAGE_UNIFIED_TRACEFILE}
    VERBATIM
    COMMENT "Generating Cobertura report"
    )

  add_custom_target(
    coverage-cobertura
    DEPENDS "${COVERAGE_XML_REPORT}"
  )
  set_target_properties(
    coverage-cobertura PROPERTIES ADDITIONAL_CLEAN_FILES ${COVERAGE_XML_REPORT}
  )
endmacro()
