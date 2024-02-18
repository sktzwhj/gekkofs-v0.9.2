#!/usr/bin/env bash
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

## Reduce the size of CI artifacts
BUILDDIR=$1

usage() {
    echo -e "ERROR: Missing build directory\n"
    echo "  Usage: $(basename $0) BUILD_DIR"
    exit 1
}

if [[ -z "${BUILDDIR}" ]]; then
    usage
fi

echo "Cleaning up ${BUILDDIR} (size: $(du -sh ${BUILDDIR} | cut -d '	' -f 1)):"
echo "  * Removing object files"

find ${BUILDDIR} \
    \( \
        -name "*.a" -or \
        -name "*.o" -or \
        -name "*.pdb" \
    \) \
    -delete

echo "  * Removing already-installed binaries:"

find ${BUILDDIR} \
    \( \
        -name "libgkfs_intercept.so" -or \
        -name "gkfs_daemon" \
    \) \
    -delete

echo "Finished (${BUILDDIR} size: $(du -sh ${BUILDDIR} | cut -d '	' -f 1))"
