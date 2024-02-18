#!/bin/bash
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage_short() {
	echo "
usage: compile_dep.sh -h |
                      -p PROFILE_NAME[:PROFILE_VERSION] |
                      -d DEPENDENCY_NAME[[@PROFILE_NAME][:PROFILE_VERSION]] |
                      -l [PROFILE_NAME:[PROFILE_VERSION]] |
                      -h
                      [ -P PROFILES_DIR ] [ -j COMPILE_CORES] [ -n ] [ -v ]
                      SOURCES_PATH INSTALL_PATH
	"
}

help_msg() {

    usage_short
    echo "
This script downloads and compiles all GekkoFS dependencies (excluding the fs itself)

positional arguments:
    SOURCES_PATH    path to the downloaded sources for the dependencies
    INSTALL_PATH    path to the installation directory for the built dependencies


optional arguments:
    -h, --help  Shows this help message and exits
    -l, --list-dependencies [[PROFILE_NAME:]PROFILE_VERSION]
                        List dependencies available for building and installation
    -p, --profile PROFILE_NAME[:PROFILE_VERSION]
                        Allows installing a pre-defined set of dependencies as defined
                        in \${PROFILES_DIR}/PROFILE_NAME.specs. This is useful to
                        deploy specific library versions and/or configurations,
                        using a recognizable name. Optionally, PROFILE_NAME may include
                        a specific version for the profile, e.g. 'mogon2:latest' or
                        'ngio:0.9.2', which will download the dependencies defined for
                        that specific version. If unspecified, the 'default:latest' profile
                        will be used, which should include all the possible dependencies.
    -d, --dependency DEPENDENCY_NAME[[@PROFILE_NAME][:PROFILE_VERSION]]
                        Build and install a specific dependency, ignoring any --profile
                        option provided. If PROFILE_NAME is unspecified, the 'default'
                        profile will be used. Similarly, if PROFILE_VERSION is
                        unspecified, the 'latest' version of the specified profile will
                        be used.
    -j, --compilecores COMPILE_CORES
                        Set the number of cores that will be used to compile the
                        dependencies. If unspecified, defaults to the number of
                        available cores.
    -t, --test  Perform libraries tests.
    -P, --profiles-dir PROFILES_DIR
                        Choose the directory to be used when searching for profiles.
                        If unspecified, PROFILES_DIR defaults to \${PWD}/profiles.
    -n, --dry-run
                        Do not actually run, print only what would be done.
    -v, --verbose       Increase download verbosity
"
}
PROFILE=""
DEPENDENCY=""
VERBOSE=""
DL_ARGS=""
COMPILE_ARGS=""

POSITIONAL=()
while [[ $# -gt 0 ]]; do
    key="$1"

    case ${key} in
    -p | --profile)
        PROFILE="$2"
        shift # past argument
        shift # past value
        ;;
    -d | --dependency)
        DEPENDENCY="$2"
        shift # past argument
        shift # past value
        ;;
    -j | --compilecores)
        COMPILE_ARGS="$COMPILE_ARGS -j $2"
        shift # past argument
        shift # past value
        ;;
    -t | --test)
        COMPILE_ARGS="$COMPILE_ARGS -t"
        shift
        ;;
    -P | --profiles-dir)
        DL_ARGS="$DL_ARGS -P $2"
        COMPILE_ARGS="$COMPILE_ARGS -P $2"
        shift # past argument
        shift # past value
        ;;
    -l | --list-dependencies)
        "${SCRIPT_DIR}"/dl_dep.sh -l
        exit
        ;;
    -v | --verbose)
        DL_ARGS="$DL_ARGS -v"
        VERBOSE=true
        shift # past argument
        ;;
    -h | --help)
        help_msg
        exit
        ;;
    -n | --dry-run)
        DL_ARGS="$DL_ARGS -n"
        COMPILE_ARGS="$COMPILE_ARGS -n"
        shift
        ;;
    *) # unknown option
        POSITIONAL+=("$1") # save it in an array for later
        shift              # past argument
        ;;
    esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

# deal with positional arguments
if [[ (-z ${1+x}) || (-z ${2+x}) ]]; then
    echo "ERROR: Positional arguments missing."
    usage_short
    exit 1
fi

SOURCE_DIR="$(readlink -mn "${1}")"
INSTALL_DIR="$(readlink -mn "${2}")"
# build arguments
if [[ -n $DEPENDENCY ]]; then
    PROFILE_STR="-d $DEPENDENCY"
    if [[ -n $PROFILE ]]; then
        PROFILE_STR="$PROFILE_STR@$PROFILE"
    fi
elif [[ -n $PROFILE ]]; then
    PROFILE_STR="-p $PROFILE"
fi
if [[ -n $PROFILE_STR ]]; then
    DL_ARGS="$DL_ARGS $PROFILE_STR"
    COMPILE_ARGS="$COMPILE_ARGS $PROFILE_STR"
fi

DL_CMD="$SCRIPT_DIR/dl_dep.sh $DL_ARGS $SOURCE_DIR"
COMPILE_CMD="$SCRIPT_DIR/compile_dep.sh $COMPILE_ARGS $SOURCE_DIR $INSTALL_DIR"

echo "################################"
echo "# Downloading dependencies ... #"
echo "################################"
[[ "$VERBOSE" == true ]] && echo "# DL_CMD: $DL_CMD"
$DL_CMD || exit 1

echo -e "\n\n##############################"
echo "# Compiling dependencies ... #"
echo "##############################"
[[ "$VERBOSE" == true ]] && echo "# COMPILE_CMD: $COMPILE_CMD"
$COMPILE_CMD || exit 1