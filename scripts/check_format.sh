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

PROJECT_SRC="$(pwd)/src"
PROJECT_INCLUDE="$(pwd)/include"
RUN_FORMAT=false
CLANG_FORMAT_BIN=""
VERBOSE=false
# Mac special treatment for readlink. greadlink must be installed...
READLINK_BIN=$(which readlink)
case "$(uname -s)" in
    Linux*)     READLINK_BIN=$(which readlink);;
    Darwin*)    READLINK_BIN=$(which greadlink);;
    *)          READLINK_BIN="readlink"
esac

usage_short() {
    echo "
usage: check_format.sh [-h] [-s <PROJECT_SRC>] [-i <PROJECT_INCLUDE>] [-c <CLANG_FORMAT_BIN>] [--run_format]
	"
}

help_msg() {

    usage_short
    echo "
This script checks if the code is formatted correctly by the rules set in .clang-format

optional arguments:
    -h, --help          shows this help message and exits
    -s <PROJECT_SRC>, --src <PROJECT_SRC>
                        path to the src/ directory of the project
                        (default: 'pwd/src')
    -i <PROJECT_INCLUDE>, --include <PROJECT_INCLUDE>
                        path to the include/ directory of the project
                        (default: 'pwd/include')
    -c <CLANG_FORMAT_BIN>, --clang_format_path <CLANG_FORMAT_BIN>
                        path to clang-format binary
                        (default: looks for 'clang-format' or 'clang-format-10')
    -r, --run_format    run clang-formatter before formatting check
                        DISCLAIMER: FILES ARE MODIFIED IN PLACE!
    -v, --verbose       shows the diff of all files
"
}

POSITIONAL=()
while [[ $# -gt 0 ]]; do
    key="$1"

    case ${key} in
    -s | --src)
        PROJECT_SRC="$($READLINK_BIN -mn "${2}")"
        shift # past argument
        shift # past value
        ;;
    -i | --include)
        PROJECT_INCLUDE="$($READLINK_BIN -mn "${2}")"
        shift # past argument
        shift # past value
        ;;
    -c | --clang_format_path)
        CLANG_FORMAT_BIN="$($READLINK_BIN -mn "${2}")"
        if [[ ! -x $CLANG_FORMAT_BIN ]]; then
            echo "*** ERR: clang-format path $CLANG_FORMAT_BIN is not an executable! Exiting ..."
            exit 1
        fi
        shift # past argument
        shift # past value
        ;;
    -r | --run_format)
        RUN_FORMAT=true
        shift # past argument
        ;;
    -v | --verbose)
        VERBOSE=true
        shift # past argument
        ;;
    -h | --help)
        help_msg
        exit
        ;;
    *) # unknown option
        POSITIONAL+=("$1") # save it in an array for later
        shift              # past argument
        ;;
    esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

# check for clang-format executable if it hasn't been set by the user
if [[ -z $CLANG_FORMAT_BIN ]]; then
    CLANG_FORMAT_BIN=$(command -v clang-format)
    if [[ -z $CLANG_FORMAT_BIN ]]; then
        CLANG_FORMAT_BIN=$(command -v clang-format-15)
        # if it still doesn't exist exit
        if [[ -z $CLANG_FORMAT_BIN ]]; then
            echo "*** ERR: clang-format not found! Exiting ..."
            exit 1
        fi
    fi
fi
echo "* Using clang-format binary: $CLANG_FORMAT_BIN"
echo "* Using version: $($CLANG_FORMAT_BIN --version)"

FAIL=false

echo "* Source directory: $PROJECT_SRC"
if [[ ! -d "$PROJECT_SRC" ]]; then
    echo "*** ERR: $PROJECT_SRC does not exist."
    exit 1
fi
echo "* Include directory: $PROJECT_INCLUDE"
if [[ ! -d "$PROJECT_INCLUDE" ]]; then
    echo "*** ERR: $PROJECT_INCLUDE does not exist."
    exit 1
fi
echo "* Checking for unformatted files ..."
if [[ "$RUN_FORMAT" == true ]]; then
    echo "*** Running clang-format on all files and formatting them in place."
    echo "* The following files and number of lines were formatted:"
else
    echo "* The following files and number of lines need to be formatted:"
fi
echo "------------------------------------------"
while IFS= read -r -d '' FILE; do
    UNFORMATTED_LINES=$(diff -u <(cat "$FILE") <($CLANG_FORMAT_BIN -style=file "$FILE") | wc -l)
    if ((UNFORMATTED_LINES > 0)); then
        if [[ "$RUN_FORMAT" == true ]]; then
            echo "*** Reformatting $UNFORMATTED_LINES lines in '$FILE'"
            $CLANG_FORMAT_BIN -i -style=file "$FILE"
        else
            echo -n "$FILE "
            echo "$UNFORMATTED_LINES"
            if [[ "$VERBOSE" == true ]]; then
                diff -u <(cat "$FILE") <($CLANG_FORMAT_BIN -style=file "$FILE")
                echo "_______________________________________________________"
            fi
            echo
            FAIL=true
        fi
    fi
done < <(find "$PROJECT_SRC" "$PROJECT_INCLUDE" -type f \( -name "*.cpp" -o -name "*.hpp" \) -print0)

echo "------------------------------------------"
if [[ "$FAIL" == true ]]; then
    echo "* Format check failed! Format your code with 'clang-format -style=file' or pass '-r' to reformat files in place."
    exit 1
fi

echo "* Format check successful."
