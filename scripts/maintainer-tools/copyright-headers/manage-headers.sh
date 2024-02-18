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

PROJECT_DIR=$(readlink -f .)
PROJECT_CONFIG_FILE="project.config"
DOCKER_DIR="/usr/src"

INCLUDE_PATTERNS=( "*.c" "*.h" "*.cpp" "*.hpp" "*.am" "*.ac" "*.py" "*.sh" "*.mk")
EXCLUDE_PATTERNS=( ".git" "*build*" "*externals*" "*spdlog*" "*ctypesgen*" "./tests/catch.hpp" )

function help() {

cat << EOF
Usage: $(basename "$0") COMMAND [OPTIONS] [PATH]...

Add or remove license headers for each PATH specified by the user. If
no PATH is specified, the specified command is recursively applied to all files
in the current directory matching the following __default__ expressions:

  INCLUDE_PATTERNS: ${INCLUDE_PATTERNS[@]}
  EXCLUDE_PATTERNS: ${EXCLUDE_PATTERNS[@]}

These default patterns can be overriden with a '${PROJECT_CONFIG_FILE}' file.

The following commands can be used:
  -a, --add               Add license header to targets.
  -r, --remove            Remove license header from targets.
  -h, --help              Show this help message, then exit.

Additionally, the following options are supported:
  -p, --project-dir DIR   The root directory of the project's target files.
                          Defaults to '\$PWD', the current directory.
  -c, --config-file FILE  The configuration file used for this project.
                          Defaults to '\$PWD/project.config'.
  -s, --show-targets      Don't actually execute, just show which targets would
                          be considered.
  -n, --dry-run           Don't actually execute, just show what would happen.
EOF
    exit 1
}

function parse_args() {

    show_targets_only=false

    # $@ is all command line parameters passed to the script.
    # -o is for short options like -v
    # -l is for long options with double dash like --version
    # the comma separates different long options
    options=$(getopt -l \
                "add,remove,help,project-dir:,config-file:,dry-run,show-targets" \
                -o "arhp:c:ns" -- "$@")

    # set --:
    # If no arguments follow this option, then the positional parameters are
    # unset. Otherwise, the positional parameters are set to the arguments,
    # even if some of them begin with a ‘-’.
    eval set -- "${options}"

    while true;
    do
        OPT="$1"
        case "${OPT}" in
            -a | --add)
                PARSED_COMMAND="--add"
                ;;

            -r | --remove)
                PARSED_COMMAND="--remove"
                ;;


            -p | --project-dir)
                shift
                if [[ -z "$1" ]]; then
                    echo "option '${OPT}' requires an argument"
                    exit 1
                fi

                if ! [[ -d $1 ]]; then
                    echo "directory '${1}' does not exist."
                    exit 1
                fi

                PROJECT_DIR=$(readlink -f "$1")
                ;;

            -c | --config-file)
                shift
                if [[ -z "$1" ]]; then
                    echo "option '${OPT}' requires an argument"
                    exit 1
                fi

                if ! [[ -f $1 ]]; then
                    echo "file '${1}' does not exist."
                    exit 1
                fi

                PROJECT_CONFIG_FILE=$(readlink -f "$1")
                ;;

            -n | --dry-run)
                EXTRA_PROG_ARGS+=( "-n" )
                ;;

            -s | --show-targets)
                show_targets_only=true
                ;;

            --)
                shift
                EXTRA_SCRIPT_ARGS=( "$@" )
                break
                ;;

            -h | --help | *)
                help
                exit 0
                ;;
        esac
        shift
    done

    echo "-- Project directory: '${PROJECT_DIR}' --"

    if [[ -z "${PARSED_COMMAND}" ]]; then
        echo "$(basename "$0"): ERROR: no command provided"
        help
        exit 1
    fi
}

function validate_gem_version() {

    if shopt -qo xtrace; then
        return
    fi

    IFS=' .' read -r -a VERSION < <( (${COPYRIGHT_PROG} -v 2>&1) |& head -n1)

    if [[ "${VERSION[1]}" -lt 1 ]] ||
       [[ "${VERSION[2]}" -lt 0 ]] ||
       [[ "${VERSION[3]}" -lt 27 ]]; then
        echo "CopyrightHeader gem too old"
        exit 1
    fi
}

function find_command() {

    COPYRIGHT_LICENSE_HOST=$(readlink -f "${COPYRIGHT_LICENSE}")
    COPYRIGHT_LICENSE_DOCKER="${DOCKER_DIR}/"$(basename "${COPYRIGHT_LICENSE}")

    PROG_ARGS=(
        "--rm"
        "--volume=${PROJECT_DIR}:${PROJECT_DIR}"
        "--volume=${COPYRIGHT_LICENSE_HOST}:${COPYRIGHT_LICENSE_DOCKER}"
    )

    if [[ -n "${COPYRIGHT_SYNTAX}" ]]; then
        COPYRIGHT_SYNTAX_HOST=$(readlink -f "${COPYRIGHT_SYNTAX}")
        COPYRIGHT_SYNTAX_DOCKER="${DOCKER_DIR}/"$(basename "${COPYRIGHT_SYNTAX}")
        PROG_ARGS+=( "--volume=${COPYRIGHT_SYNTAX_HOST}:${COPYRIGHT_SYNTAX_DOCKER}" )
    fi

    COPYRIGHT_PROG="docker run ${PROG_ARGS[*]} gekkofs/copyright-header:latest"

    validate_gem_version
}

function print_target() {

    target="$1"
    message="$2"
    prefix="Checking "

    target_length="${#target}"
    message_length="${#message}"
    prefix_length="${#prefix}"

    maxcols=$(echo "scale=0; ($(tput cols)*0.6)/1" | bc -l)

    # the +2 and -2 accounts for quotes used when printing targets
    # the +1 for the separation space
    if [[ $((target_length + prefix_length + 2 + 1 + message_length)) -gt $maxcols ]]; then
        printf "$prefix%s\n%$(( maxcols ))s\n" "'${t}'" "${message}"
    else
        printf "$prefix%s %$(( maxcols - prefix_length - target_length - 3))s\n" "'${t}'" "${message}"
    fi
}

function find_targets() {

    TARGETS=()

    # option1: targets are passed as arguments
    if [[ -n "$*" ]]; then
        TARGETS=( "$@" )

        for t in "${TARGETS[@]}";
        do
            t="$(readlink -f ${t})"
            if [ "${t##$PROJECT_DIR}" != "${PROJECT_DIR}" ]; then
                print_target "${t}" "[SKIPPED -- outside project directory]"
            else
                PATH_ARGS+="${t}:"
            fi
        done

        return
    fi

    # option 2: targets are auto-detected based on ${PROJECT_DIR},
    # ${INCLUDE_PATTERNS}, and ${EXCLUDE_PATTERNS}
    if [[ ${#EXCLUDE_PATTERNS[@]} -ne 0 ]]; then
        for p in "${EXCLUDE_PATTERNS[@]}"; 
        do 
            exclude_args+=( -not \( -path "${p}" -prune \) )
        done
    fi

    if [[ ${#INCLUDE_PATTERNS[@]} -ne 0 ]]; then

        include_args=( \( -path "${INCLUDE_PATTERNS[0]}" \) )

        for ((i = 1; i < ${#INCLUDE_PATTERNS[@]}; ++i));
        do 
            include_args+=( -o \( -path "${INCLUDE_PATTERNS[$i]}" \) )
        done
    fi

    #### see https://stackoverflow.com/questions/23356779/how-can-i-store-the-find-command-results-as-an-array-in-bash
    while IFS= read -r -d $'\0';
    do
        TARGETS+=("$REPLY")
    done < <(find "${PROJECT_DIR}" \( "${exclude_args[@]}" \) -and \( "${include_args[@]}" \) -print0)

    for t in "${TARGETS[@]}";
    do
        if ! git ls-files --error-unmatch "${t}" > /dev/null 2>&1; then
            print_target "${t}" "[SKIPPED -- not under version control]"
        else
            print_target "${t}" "[OK]"
            PATH_ARGS+="${t}:"
        fi
    done

    if $show_targets_only; then
        exit 0
    fi
}

function read_project_config() {

    ## read project configuration
    if [[ ! -f "${PROJECT_CONFIG_FILE}" ]];
    then
        echo "ERROR: Missing project configuration file."
        exit 1
    fi

    source "${PROJECT_CONFIG_FILE}"

    ## verify mandatory options
    if [[ -z "${COPYRIGHT_LICENSE}" ]]; then
        echo "ERROR: missing COPYRIGHT_LICENSE in '${PROJECT_CONFIG_FILE}'"
        exit 1
    fi
}

function prepare_command_args() {

    ARGS=()

    # mandatory args
    ARGS+=( "--guess-extension" )
    ARGS+=( "--license-file=${COPYRIGHT_LICENSE}" )
    ARGS+=( "--output-dir=/" )

    # optional
    if [[ -n "${COPYRIGHT_SYNTAX}" ]];
    then
        ARGS+=( "--syntax=${COPYRIGHT_SYNTAX}" )
    fi

    if [[ -n "${COPYRIGHT_SOFTWARE}" ]];
    then
        ARGS+=( "--copyright-software=\"${COPYRIGHT_SOFTWARE}\"" )
    fi

    if [[ -n "${COPYRIGHT_DESCRIPTION}" ]];
    then
        ARGS+=( "--copyright-software-description=\"${COPYRIGHT_DESCRIPTION}\"" )
    fi

    if [[ -n "${COPYRIGHT_YEARS}" ]];
    then
        ARGS+=( "--copyright-year=${COPYRIGHT_YEARS}" )
    fi

    if [[ -n "${COPYRIGHT_WORD_WRAP}" ]];
    then
        ARGS+=( "--word-wrap=${COPYRIGHT_WORD_WRAP}" )
    fi

    COPYRIGHT_PROG_ARGS=( "${ARGS[@]}" )

}

function add_header() {

    # load project configuration
    read_project_config

    find_targets "$@"

    if [[ -z $PATH_ARGS ]] ; then
        echo No targets detected. Nothing to do.
        exit 0
    fi

    # prepare command to execute and its args
    find_command
    prepare_command_args

    # execute command
    ${COPYRIGHT_PROG} \
        --add-path "${PATH_ARGS::-1}" \
        "${COPYRIGHT_PROG_ARGS[@]}" \
        "${EXTRA_PROG_ARGS[@]}"

    exit $?
}

function remove_header() {

    # load project configuration
    read_project_config

    find_targets "$@"

    if [[ -z $PATH_ARGS ]]; then
        echo No targets detected. Nothing to do.
        exit 0
    fi

    # prepare command to execute and its args
    find_command
    prepare_command_args

    ${COPYRIGHT_PROG} \
        --remove-path "${PATH_ARGS::-1}" \
        "${COPYRIGHT_PROG_ARGS[@]}" \
        "${EXTRA_PROG_ARGS[@]}"

    exit $?
}


################################################################################
### Script starts here                                                       ###
################################################################################

# process command line args
if [[ $# -eq 0 ]]; then
    help
fi

parse_args "$@"

# execute selected command
case $PARSED_COMMAND in
    -a | --add)
        add_header "${EXTRA_SCRIPT_ARGS[@]}"
        ;;

    -r | --remove)
        remove_header "${EXTRA_SCRIPT_ARGS[@]}"
        ;;
esac

exit 0
