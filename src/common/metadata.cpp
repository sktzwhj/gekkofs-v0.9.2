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

#include <common/metadata.hpp>
#include <config.hpp>

#include <fmt/format.h>

extern "C" {
#include <sys/stat.h>
#include <unistd.h>
}

#include <ctime>
#include <cassert>
#include <random>

namespace gkfs::metadata {

static const char MSP = '|'; // metadata separator

/**
 * Generate a unique ID for a given path
 * @param path
 * @return unique ID
 */
uint16_t
gen_unique_id(const std::string& path) {
    // Generate a random salt value using a pseudo-random number generator
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0,
                                        std::numeric_limits<uint16_t>::max());
    auto salt = static_cast<uint16_t>(dis(gen));

    // Concatenate the identifier and salt values into a single string
    auto input_str = fmt::format("{}{}", path, salt);

    // Use std::hash function to generate a hash value from the input string
    std::hash<std::string> const hasher;
    auto hash_value = hasher(input_str);

    // Use the lower 16 bits of the hash value as the unique ID
    return static_cast<uint16_t>(hash_value & 0xFFFF);
}

inline void
Metadata::init_time() {
    if constexpr(gkfs::config::metadata::use_ctime) {
        ctime_ = std::time(nullptr);
    }
    if constexpr(gkfs::config::metadata::use_mtime) {
        mtime_ = std::time(nullptr);
    }
    if constexpr(gkfs::config::metadata::use_atime) {
        atime_ = std::time(nullptr);
    }
}

Metadata::Metadata(const mode_t mode)
    : mode_(mode), link_count_(0), size_(0), blocks_(0) {
    assert(S_ISDIR(mode_) || S_ISREG(mode_));
    init_time();
}

#ifdef HAS_SYMLINKS

Metadata::Metadata(const mode_t mode, const std::string& target_path)
    : mode_(mode), link_count_(0), size_(0), blocks_(0),
      target_path_(target_path) {
    assert(S_ISLNK(mode_) || S_ISDIR(mode_) || S_ISREG(mode_));
    // target_path should be there only if this is a link
    assert(target_path_.empty() || S_ISLNK(mode_));
    // target_path should be absolute
    assert(target_path_.empty() || target_path_[0] == '/');
    init_time();
}

#endif

Metadata::Metadata(const std::string& binary_str) {
    size_t read = 0;

    auto ptr = binary_str.data();
    mode_ = static_cast<unsigned int>(std::stoul(ptr, &read));
    // we read something
    assert(read > 0);
    ptr += read;

    // last parsed char is the separator char
    assert(*ptr == MSP);
    // yet we have some character to parse

    size_ = std::stol(++ptr, &read);
    assert(read > 0);
    ptr += read;

    // The order is important. don't change.
    if constexpr(gkfs::config::metadata::use_atime) {
        assert(*ptr == MSP);
        atime_ = static_cast<time_t>(std::stol(++ptr, &read));
        assert(read > 0);
        ptr += read;
    }
    if constexpr(gkfs::config::metadata::use_mtime) {
        assert(*ptr == MSP);
        mtime_ = static_cast<time_t>(std::stol(++ptr, &read));
        assert(read > 0);
        ptr += read;
    }
    if constexpr(gkfs::config::metadata::use_ctime) {
        assert(*ptr == MSP);
        ctime_ = static_cast<time_t>(std::stol(++ptr, &read));
        assert(read > 0);
        ptr += read;
    }
    if constexpr(gkfs::config::metadata::use_link_cnt) {
        assert(*ptr == MSP);
        link_count_ = static_cast<nlink_t>(std::stoul(++ptr, &read));
        assert(read > 0);
        ptr += read;
    }
    if constexpr(gkfs::config::metadata::use_blocks) { // last one will not
                                                       // encounter a
                                                       // delimiter anymore
        assert(*ptr == MSP);
        blocks_ = static_cast<blkcnt_t>(std::stol(++ptr, &read));
        assert(read > 0);
        ptr += read;
    }

#ifdef HAS_SYMLINKS
    // Read target_path
    assert(*ptr == MSP);
    target_path_ = ++ptr;
    // target_path should be there only if this is a link
    ptr += target_path_.size();
#ifdef HAS_RENAME
    // Read rename target, we had captured '|' so we need to recover it
    if(!target_path_.empty()) {
        auto index = target_path_.find_last_of(MSP);
        auto size = target_path_.size();
        target_path_ = target_path_.substr(0, index);
        ptr -= (size - index);
    }
    assert(*ptr == MSP);
    rename_path_ = ++ptr;
    ptr += rename_path_.size();
#endif // HAS_RENAME
#endif // HAS_SYMLINKS

    // we consumed all the binary string
    assert(*ptr == '\0');
}

std::string
Metadata::serialize() const {
    std::string s;
    // The order is important. don't change.
    s += fmt::format_int(mode_).c_str(); // add mandatory mode
    s += MSP;
    s += fmt::format_int(size_).c_str(); // add mandatory size
    if constexpr(gkfs::config::metadata::use_atime) {
        s += MSP;
        s += fmt::format_int(atime_).c_str();
    }
    if constexpr(gkfs::config::metadata::use_mtime) {
        s += MSP;
        s += fmt::format_int(mtime_).c_str();
    }
    if constexpr(gkfs::config::metadata::use_ctime) {
        s += MSP;
        s += fmt::format_int(ctime_).c_str();
    }
    if constexpr(gkfs::config::metadata::use_link_cnt) {
        s += MSP;
        s += fmt::format_int(link_count_).c_str();
    }
    if constexpr(gkfs::config::metadata::use_blocks) {
        s += MSP;
        s += fmt::format_int(blocks_).c_str();
    }

#ifdef HAS_SYMLINKS
    s += MSP;
    s += target_path_;
#ifdef HAS_RENAME
    s += MSP;
    s += rename_path_;
#endif // HAS_RENAME
#endif // HAS_SYMLINKS

    return s;
}

void
Metadata::update_atime_now() {
    auto time = std::time(nullptr);
    atime_ = time;
}

void
Metadata::update_mtime_now() {
    auto time = std::time(nullptr);
    mtime_ = time;
}

//-------------------------------------------- GETTER/SETTER

time_t
Metadata::atime() const {
    return atime_;
}

void
Metadata::atime(time_t atime) {
    Metadata::atime_ = atime;
}

time_t
Metadata::mtime() const {
    return mtime_;
}

void
Metadata::mtime(time_t mtime) {
    Metadata::mtime_ = mtime;
}

time_t
Metadata::ctime() const {
    return ctime_;
}

void
Metadata::ctime(time_t ctime) {
    Metadata::ctime_ = ctime;
}

mode_t
Metadata::mode() const {
    return mode_;
}

void
Metadata::mode(mode_t mode) {
    Metadata::mode_ = mode;
}

nlink_t
Metadata::link_count() const {
    return link_count_;
}

void
Metadata::link_count(nlink_t link_count) {
    Metadata::link_count_ = link_count;
}

size_t
Metadata::size() const {
    return size_;
}

void
Metadata::size(size_t size) {
    Metadata::size_ = size;
}

blkcnt_t
Metadata::blocks() const {
    return blocks_;
}

void
Metadata::blocks(blkcnt_t blocks) {
    Metadata::blocks_ = blocks;
}

#ifdef HAS_SYMLINKS

std::string
Metadata::target_path() const {
    return target_path_;
}

void
Metadata::target_path(const std::string& target_path) {
    target_path_ = target_path;
}

bool
Metadata::is_link() const {
    return S_ISLNK(mode_);
}

#ifdef HAS_RENAME
std::string
Metadata::rename_path() const {
    return rename_path_;
}

void
Metadata::rename_path(const std::string& rename_path) {
    rename_path_ = rename_path;
}

#endif // HAS_RENAME
#endif // HAS_SYMLINKS

} // namespace gkfs::metadata
