/*
  Copyright 2018-2024, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2024, Johannes Gutenberg Universitaet Mainz, Germany

  This software was partially supported by the
  EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).

  This software was partially supported by the
  ADA-FS project under the SPPEXA project funded by the DFG.

  This file is part of GekkoFS.

  GekkoFS is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  GekkoFS is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with GekkoFS.  If not, see <https://www.gnu.org/licenses/>.

  SPDX-License-Identifier: GPL-3.0-or-later
*/

#include <common/path_util.hpp>

#include <system_error>
#include <cstring>
#include <cassert>

using namespace std;

namespace gkfs::path {

bool
is_relative(const string& path) {
    return (!path.empty()) && (path.front() != separator);
}

bool
is_absolute(const string& path) {
    return (!path.empty()) && (path.front() == separator);
}

bool
has_trailing_slash(const string& path) {
    return (!path.empty()) && (path.back() == separator);
}

/** Add path prefix to a given C string.
 *
 * Returns a string composed by the `prefix_path`
 * followed by `raw_path`.
 *
 * This would return the same of:
 * ```
 * string(raw_path).append(prefix_path);
 * ```
 * But it is faster because it avoids to copy the `raw_path` twice.
 *
 *
 * Time cost approx: O(len(prefix_path)) + 2 O(len(raw_path))
 *
 * Example:
 * ```
 * prepend_path("/tmp/prefix", "./my/path") == "/tmp/prefix/./my/path"
 * ```
 */
string
prepend_path(const string& prefix_path, const char* raw_path) {
    assert(!has_trailing_slash(prefix_path));
    ::size_t raw_len = ::strlen(raw_path);
    string res;
    res.reserve(prefix_path.size() + 1 + raw_len);
    res.append(prefix_path);
    res.push_back(separator);
    res.append(raw_path, raw_len);
    return res;
}

/** Split a path into its components
 *
 * Returns a vector of the components of the given path.
 *
 * Example:
 *  split_path("/first/second/third") == ["first", "second", "third"]
 */
::vector<string>
split_path(const string& path) {
    ::vector<string> tokens;
    size_t start = string::npos;
    size_t end = (path.front() != separator) ? 0 : 1;
    while(end != string::npos && end < path.size()) {
        start = end;
        end = path.find(separator, start);
        tokens.push_back(path.substr(start, end - start));
        if(end != string::npos) {
            ++end;
        }
    }
    return tokens;
}


/** Make an absolute path relative to a root path
 *
 * Convert @absolute_path into a relative one with respect to the given
 * @root_path. If @absolute_path do not start at the given @root_path an empty
 * string will be returned. NOTE: Trailing slash will be stripped from the new
 * constructed relative path.
 */
string
absolute_to_relative(const string& root_path, const string& absolute_path) {
    assert(is_absolute(root_path));
    assert(is_absolute(absolute_path));
    assert(!has_trailing_slash(root_path));

    auto diff_its = ::mismatch(absolute_path.cbegin(), absolute_path.cend(),
                               root_path.cbegin());
    if(diff_its.second != root_path.cend()) {
        // complete path doesn't start with root_path
        return {};
    }

    // iterator to the starting char of the relative portion of the
    // @absolute_path
    auto rel_it_begin = diff_its.first;
    // iterator to the end of the relative portion of the @absolute_path
    auto rel_it_end = absolute_path.cend();

    // relative path start exactly after the root_path prefix
    assert((size_t) (rel_it_begin - absolute_path.cbegin()) ==
           root_path.size());

    if(rel_it_begin == rel_it_end) {
        // relative path is empty, @absolute_path was equal to @root_path
        return {'/'};
    }

    // remove the trailing slash from relative path
    if(has_trailing_slash(absolute_path) &&
       rel_it_begin !=
               rel_it_end -
                       1) { // the relative path is longer then 1 char ('/')
        --rel_it_end;
    }

    return {rel_it_begin, rel_it_end};
}

/**
 * returns the directory name for given path
 * @param path
 * @return
 */
string
dirname(const string& path) {
    assert(path.size() > 1 || path.front() == separator);
    assert(path.size() == 1 || !has_trailing_slash(path));

    auto parent_path_size = path.find_last_of(separator);
    assert(parent_path_size != string::npos);
    if(parent_path_size == 0) {
        // parent is '/'
        parent_path_size = 1;
    }
    return path.substr(0, parent_path_size);
}

} // namespace gkfs::path