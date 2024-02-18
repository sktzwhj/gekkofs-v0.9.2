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
DEPENDENCY=""
CORES=""
SOURCE_DIR=""
INSTALL_DIR=""
PERFORM_TEST=

EXECUTION_MODE=
DRY_RUN=false

DEFAULT_PROFILE="default"
DEFAULT_VERSION="latest"
PROFILE_NAME=${DEFAULT_PROFILE}
PROFILE_VERSION=${DEFAULT_VERSION}
PROFILES_DIR="${SCRIPT_DIR}/profiles"
declare -a PROFILE_DEP_LIST
declare -A PROFILE_DEP_NAMES
declare -A PROFILE_WGETDEPS PROFILE_CLONEDEPS PROFILE_SOURCES PROFILE_EXTRA_INSTALL_ARGS
declare -A PROFILE_CLONEDEPS_ARGS PROFILE_CLONEDEPS_PATCHES

usage_short() {
	echo "
usage: compile_dep.sh -h |
                      -p PROFILE_NAME[:PROFILE_VERSION] |
                      -d DEPENDENCY_NAME[[@PROFILE_NAME][:PROFILE_VERSION]] |
                      -l [PROFILE_NAME:[PROFILE_VERSION]] |
                      -h
                      [ -P PROFILES_DIR ] [ -j COMPILE_CORES] [ -n ]
                      SOURCES_PATH INSTALL_PATH
	"
}

help_msg() {

    usage_short
    echo "
This script compiles all GekkoFS dependencies (excluding the fs itself)

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
                'ngio:0.9.1', which will download the dependencies defined for
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
"
}

exec_mode_error() {
    echo "ERROR: --profile and --dependency options are mutually exclusive"
    usage_short
    exit 1
}

list_versions() {

    if [[ ! -d "${PROFILES_DIR}" ]]; then
        echo "Directory '${PROFILES_DIR}' does not exist. No profiles available."
        exit 1
    fi

    declare -A versions

    while IFS= read -r -d '' filename; do
        id="$(basename $(dirname ${filename}))"
        profile="$(basename ${filename%%.specs})"

        versions[$id]+="${profile} "
    done < <(find -L "${PROFILES_DIR}" -type f -name "*.specs" -print0 | sort -z)

    echo -e "Available versions and configuration profiles:\n"

    for id in "${!versions[@]}"; do
        echo "  ${id}:"
        echo -e "    ${versions[${id}]}\n"
    done

    exit 0
}

list_profiles() {

    local TAG=$1

    if [[ "$TAG" =~ ^(.*):(.*)$ ]]; then
        PROFILE="${BASH_REMATCH[1]}.specs"

        if [[ -n ${BASH_REMATCH[2]} ]]; then
            VERSION="${BASH_REMATCH[2]}"
        else
            VERSION="latest"
        fi

    else
        VERSION="${TAG}"
    fi

    if [[ ! -d "${PROFILES_DIR}" ]]; then
        echo "Directory '${PROFILES_DIR}' does not exist. No configuration profiles found."
        exit 1
    fi

    if [[ ! -d "${PROFILES_DIR}/${VERSION}" ]]; then
        echo "Version ${VERSION} does not exist. No configuration profiles found."
        exit 1
    fi

    echo -e "Configuration profiles for '${VERSION}':\n"

    find "${PROFILES_DIR}/${VERSION}/${PROFILE}" -type f -name "*.specs" -print0 | sort -z | while IFS= read -r -d '' filename; do
        basename=$(basename "${filename}")
        version=$(basename $(dirname "${filename}"))
        profile="${basename%.*}"

        echo "* ${profile}:${version} (${filename})"

        source "${filename}"

        if [[ -n "${comment}" ]]; then
            echo -e "\n  ${comment}\n"
        fi

        for d in "${order[@]}";
        do
            if [[ -n ${wgetdeps[${d}]} ]]; then
                echo "    ${d}: ${wgetdeps[${d}]}"
            elif [[ -n ${clonedeps[${d}]} ]]; then
                echo "    ${d}: ${clonedeps[${d}]}"
            else
                echo "    ${d}: ???"
            fi
        done

        echo ""

        unset wgetdeps
        unset clonedeps
        unset clonedeps_args
        unset clonedeps_patches
        unset comment
        unset order
    done
    exit 0

}

load_profile() {

    local profile=$1
    local version=$2
    shift

    # make sure we are in a known state
    PROFILE_DEP_NAMES=()
    PROFILE_DEP_LIST=()
    PROFILE_CLONEDEPS=()
    PROFILE_CLONEDEPS_ARGS=()
    PROFILE_CLONEDEPS_PATCHES=()
    PROFILE_WGETDEPS=()

    local filename="${PROFILES_DIR}/${version}/${profile}.specs"

    if [[ ! -f "${filename}" ]]; then
        echo "Profile '${profile}:${version}' does not exist."
        exit 1
    fi

    source "${filename}"

    # some checks
    if [[ -z "${wgetdeps[*]}" && -z "${clonedeps[*]}" ]]; then
        echo "Profile '${profile}' is invalid."
        exit 1
    fi

    if [[ -z "${order[*]}" ]]; then
        echo "Profile '${profile}' is invalid."
        exit 1
    fi

    if [[ "$((${#wgetdeps[@]}+${#clonedeps[@]}))" -ne "${#order[@]}" ]]; then
        echo "Profile '${profile}' is invalid."
        exit 1
    fi

    # propagate results outside of function
    for i in "${!order[@]}"; do
        PROFILE_DEP_LIST[$i]="${order[${i}]}"
        PROFILE_DEP_NAMES["${order[$i]}"]="$i"
    done

    for k in "${!clonedeps[@]}"; do
        PROFILE_CLONEDEPS["${k}"]="${clonedeps[${k}]}"
    done

    for k in "${!clonedeps_args[@]}"; do
        PROFILE_CLONEDEPS_ARGS["${k}"]="${clonedeps_args[${k}]}"
    done

    for k in "${!clonedeps_patches[@]}"; do
        PROFILE_CLONEDEPS_PATCHES["${k}"]="${clonedeps_patches[${k}]}"
    done

    for k in "${!wgetdeps[@]}"; do
        PROFILE_WGETDEPS["${k}"]="${wgetdeps[${k}]}"
    done

    for k in "${!extra_install_args[@]}"; do
        PROFILE_EXTRA_INSTALL_ARGS["${k}"]="${extra_install_args["${k}"]}"
    done
}

prepare_build_dir() {
    if [ ! -d "$1/build" ]; then
        mkdir "$1"/build
    fi
    rm -rf "$1"/build/*
}

find_cmake() {
    local CMAKE
    CMAKE=$(command -v cmake3 || command -v cmake)
    if [ $? -ne 0 ]; then
        echo >&2 "ERROR: could not find cmake"
        exit 1
    fi
    echo "${CMAKE}"
}

determine_compiler() {

    compiler_is_gnu() {
        COMPILER_NAME="g++"

        if ! COMPILER_FULL_VERSION="$(g++ -dumpfullversion 2>&1)"; then
            echo -e "ERROR: Failed to determine compiler version."
            echo -e ">> ${COMPILER_FULL_VERSION}"
            exit 1
        fi

        COMPILER_MAJOR_VERSION="${COMPILER_FULL_VERSION%%.*}"
    }

    compiler_is_clang() {
        COMPILER_NAME="clang"

        if ! COMPILER_FULL_VERSION="$(clang -dumpversion 2>&1)"; then
            echo -e "ERROR: Failed to determine compiler version."
            echo -e ">> ${COMPILER_FULL_VERSION}"
            exit 1
        fi

        COMPILER_MAJOR_VERSION="${COMPILER_FULL_VERSION%%.*}"
    }

    # We honor the CXX environment variable if defined.
    # Otherwise, we try to find the compiler by using `command -v`.
    if [[ -n "${CXX}" && ! "${CXX}" =~ ^.*(g\+\+|clang)$ ]]; then
        echo "ERROR: Unknown compiler '${CXX}'"
        exit 1
    fi

    if [[ -n "${CXX}" && "${CXX}" =~ ^.*g\+\+$ ]]; then
        compiler_is_gnu
    elif [[ -n "${CXX}" && "$CXX" =~ ^.*clang$ ]]; then
        compiler_is_clang
    elif [[ $(command -v g++) ]]; then
        compiler_is_gnu
    elif [[ $(command -v clang) ]]; then
        compiler_is_clang
    else
        echo "ERROR: Unable to determine compiler."
        exit 1
    fi
}

# Check whether the loaded profile defines a particular dependency name.
# The function requires a valid bash regex argument to do the search. The
# function is meant to be used in a conditional context.
#
# Examples:
#   1. Check whether any flavor of 'libfabric' is defined by the profile:
#
#     if profile_has_dependency "^libfabric%.*$"; then
#        echo "libfabric found"
#     fi
#
#   2. Check whether a specific flavor of 'libfabric' is defined by the profile:
#
#     if profile_has_dependency "^libfabric%experimental$"; then
#        echo "libfabric.experimental found"
#     fi
profile_has_dependency() {

    if [[ "$#" -ne 1 ]]; then
        >&2 echo "FATAL: Missing argument in profile_has_dependency()"
        exit 1
    fi

    regex="$1"

    for name in "${PROFILE_DEP_LIST[@]}"; do

        if [[ "${name}" =~ ${regex} ]]; then
            return 0
        fi
    done

    return 1
}


POSITIONAL=()
while [[ $# -gt 0 ]]; do
    key="$1"

    case ${key} in
    -p | --profile)

        [ -n "${EXECUTION_MODE}" ] && exec_mode_error || EXECUTION_MODE='profile'

        if [[ -z "$2" ]]; then
            echo "ERROR: Missing argument for -p/--profile option"
            exit 1
        fi

        if [[ "$2" =~ ^(.*):(.*)$ ]]; then
            PROFILE_NAME="${BASH_REMATCH[1]}"
            PROFILE_VERSION="${BASH_REMATCH[2]}"
        else
            PROFILE_NAME="$2"
            PROFILE_VERSION="${DEFAULT_VERSION}"
        fi

        shift # past argument
        shift # past value
        ;;
    -d | --dependency)

        [ -n "${EXECUTION_MODE}" ] && exec_mode_error || EXECUTION_MODE='dependency'

        if [[ -z "$2" ]]; then
            echo "ERROR: Missing argument for -d/--dependency option"
            exit
        fi

        PROFILE_NAME=${DEFAULT_PROFILE}
        PROFILE_VERSION=${DEFAULT_VERSION}

        # e.g. mercury@mogon1:latest
        if [[ "$2" =~ ^(.*)@(.*):(.*)$ ]]; then
            if [[ -n "${BASH_REMATCH[1]}"  ]]; then
                DEPENDENCY="${BASH_REMATCH[1]}"
            fi

            if [[ -n "${BASH_REMATCH[2]}" ]]; then
                PROFILE_NAME="${BASH_REMATCH[2]}"
            fi

            if [[ -n "${BASH_REMATCH[3]}" ]]; then
                PROFILE_VERSION="${BASH_REMATCH[3]}"
            fi

        # e.g. mercury@mogon1
        elif [[ "$2" =~ ^(.*)@(.*)$ ]]; then
            if [[ -n "${BASH_REMATCH[1]}"  ]]; then
                DEPENDENCY="${BASH_REMATCH[1]}"
            fi

            if [[ -n "${BASH_REMATCH[2]}"  ]]; then
                PROFILE_NAME="${BASH_REMATCH[2]}"
            fi
        # e.g. mercury
        else
            DEPENDENCY="$2"
        fi

        if [[ ! -n "${DEPENDENCY}" ]]; then
            echo "ERROR: Missing dependency name."
            exit 1
        fi

        shift # past argument
        shift # past value
        ;;
    -j | --compilecores)
        CORES="$2"
        shift # past argument
        shift # past value
        ;;
    -t | --test)
        PERFORM_TEST=true
        shift
        ;;
    -P | --profiles-dir)

        if [[ ! -d "$2" ]]; then
            echo "ERROR: PROFILES_DIR '$2' does not exist or is not a directory."
            exit 1
        fi

        PROFILES_DIR="$2"
        shift # past argument
        shift # past value
        ;;
    -l | --list-dependencies)
        if [[ -z "$2" ]]; then
            list_versions
        else
            list_profiles "$2"
        fi
        exit
        ;;
    -h | --help)
        help_msg
        exit
        ;;
    -n | --dry-run)
        DRY_RUN=true
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

# deal with optional arguments
if [[ "${CORES}" == "" ]]; then
    CORES=$(grep -c ^processor /proc/cpuinfo)
    echo "CORES = ${CORES} (default)"
else
    if [[ ! "${CORES}" -gt "0" ]]; then
        echo "ERROR: CORES set to ${CORES} which is invalid.
Input must be numeric and greater than 0."
        usage_short
        exit
    else
        echo CORES = "${CORES}"
    fi
fi

load_profile "${PROFILE_NAME}" "${PROFILE_VERSION}"

CMAKE=$(find_cmake)
CMAKE="${CMAKE} -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}"

echo "Sources download path = ${SOURCE_DIR}"
echo "Installation path = ${INSTALL_DIR}"
echo "Profile name: ${PROFILE_NAME}"
echo "Profile version: ${PROFILE_VERSION}"
echo "------------------------------------"

mkdir -p "${SOURCE_DIR}" || exit 1

######### From now on exits on any error ########
set -e

export CPATH="${CPATH}:${INSTALL_DIR}/include"
export LIBRARY_PATH="${LIBRARY_PATH}:${INSTALL_DIR}/lib:${INSTALL_DIR}/lib64"
export PKG_CONFIG_PATH="${INSTALL_DIR}/lib/pkgconfig:${PKG_CONFIG_PATH}"

if [[ -n "${DEPENDENCY}" && ! -n "${PROFILE_DEP_NAMES[${DEPENDENCY}]}" ]]; then
    echo "Dependency '${DEPENDENCY}' not found in '${PROFILE_NAME}:${PROFILE_VERSION}'"
    exit 1
fi

for dep_name in "${PROFILE_DEP_LIST[@]}"; do

    # in dependency mode, skip any dependencies != DEPENDENCY
    if [[ -n "${DEPENDENCY}" && "${dep_name}" != "${DEPENDENCY}" ]]; then
        continue
    fi

    install_script="${PROFILES_DIR}/${PROFILE_VERSION}/install/${dep_name}.install"

    echo -e "\n\n######## Installing:  ${dep_name} ###############################\n"

    if [[ -f "${install_script}" ]]; then
        [[ "$DRY_RUN" == true ]] || source "${install_script}"
    else
        echo "WARNING: Install script for '${dep_name}' not found. Skipping."
        continue
    fi

    if [[ "$DRY_RUN" == false ]]; then
        determine_compiler
        pkg_install

        [ "${PERFORM_TEST}" ] && pkg_check
    fi

done

echo "Done"

exit 0
