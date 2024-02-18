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

#ifndef FS_METADATA_H
#define FS_METADATA_H
#pragma once


#include <config.hpp>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <cstdint>


namespace gkfs::metadata {

constexpr mode_t LINK_MODE = ((S_IRWXU | S_IRWXG | S_IRWXO) | S_IFLNK);

uint16_t
gen_unique_id(const std::string& path);

class Metadata {
private:
    time_t atime_{}; // access time. gets updated on file access unless mounted
                     // with noatime
    time_t mtime_{}; // modify time. gets updated when file content is modified.
    time_t ctime_{}; // change time. gets updated when the file attributes are
                     // changed AND when file content is modified.
    mode_t mode_{};
    nlink_t link_count_{}; // number of names for this inode (hardlinks)
    size_t size_{};     // size_ in bytes, might be computed instead of stored
    blkcnt_t blocks_{}; // allocated file system blocks_
#ifdef HAS_SYMLINKS
    std::string target_path_; // For links this is the path of the target file
#ifdef HAS_RENAME
    std::string rename_path_; // In some cases fd is maintained so we need the
                              // renamed path
#endif
#endif

    void
    init_time();

public:
    Metadata() = default;

    explicit Metadata(mode_t mode);

#ifdef HAS_SYMLINKS

    Metadata(mode_t mode, const std::string& target_path);

#endif

    // Construct from a binary representation of the object
    explicit Metadata(const std::string& binary_str);

    std::string
    serialize() const;

    // currently unused
    void
    update_atime_now();

    void
    update_mtime_now();

    // Getter and Setter
    time_t
    atime() const;

    void
    atime(time_t atime_);

    time_t
    mtime() const;

    void
    mtime(time_t mtime_);

    time_t
    ctime() const;

    void
    ctime(time_t ctime_);

    mode_t
    mode() const;

    void
    mode(mode_t mode_);

    nlink_t
    link_count() const;

    void
    link_count(nlink_t link_count_);

    size_t
    size() const;

    void
    size(size_t size_);

    blkcnt_t
    blocks() const;

    void
    blocks(blkcnt_t blocks_);

#ifdef HAS_SYMLINKS

    std::string
    target_path() const;

    void
    target_path(const std::string& target_path);

    bool
    is_link() const;

#ifdef HAS_RENAME
    std::string
    rename_path() const;

    void
    rename_path(const std::string& rename_path);
#endif // HAS_RENAME

#endif // HAS_SYMLINKS
};

} // namespace gkfs::metadata


#endif // FS_METADATA_H
