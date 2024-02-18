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

find_package(Sphinx REQUIRED breathe exhale)

set(EXHALE_CONTAINMENT_FOLDER api)
set(EXHALE_ROOT_FILE_NAME reference.rst)
set(EXHALE_ROOT_FILE_TITLE "Reference")

sphinx_add_docs(
  sphinx_docs
  CONF_TEMPLATE "${CMAKE_CURRENT_SOURCE_DIR}/conf.py.in"
  BREATHE_PROJECTS doxygen_docs
  BUILDER html
  SOURCE_DIRECTORY ${CMAKE_SOURCE_DIR}/docs/sphinx
  COMMENT "Generating Sphinx+Breathe documentation"
  )

# Add an install target to install the docs
# (the trailing slash is important to install the contents inside
# SPHINX_OUTPUT_DIRECTORY without including the directory itself)
include(GNUInstallDirs)
install(DIRECTORY "${SPHINX_OUTPUT_DIRECTORY}/"
  OPTIONAL
  DESTINATION ${CMAKE_INSTALL_DOCDIR})
