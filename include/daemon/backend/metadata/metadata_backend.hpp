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

#ifndef GEKKOFS_METADATA_BACKEND_HPP
#define GEKKOFS_METADATA_BACKEND_HPP

#include <memory>
#include <spdlog/spdlog.h>
#include <daemon/backend/exceptions.hpp>
#include <tuple>

namespace gkfs::metadata {


class AbstractMetadataBackend {
public:
    virtual ~AbstractMetadataBackend() = default;

    virtual std::string
    get(const std::string& key) const = 0;

    virtual void
    put(const std::string& key, const std::string& val) = 0;

    virtual void
    put_no_exist(const std::string& key, const std::string& val) = 0;

    virtual void
    remove(const std::string& key) = 0;

    virtual bool
    exists(const std::string& key) = 0;

    virtual void
    update(const std::string& old_key, const std::string& new_key,
           const std::string& val) = 0;

    virtual off_t
    increase_size(const std::string& key, size_t size, off_t offset,
                  bool append) = 0;

    virtual void
    decrease_size(const std::string& key, size_t size) = 0;

    virtual std::vector<std::pair<std::string, bool>>
    get_dirents(const std::string& dir) const = 0;

    virtual std::vector<std::tuple<std::string, bool, size_t, time_t>>
    get_dirents_extended(const std::string& dir) const = 0;

    virtual void
    iterate_all() const = 0;
};

template <typename T>
class MetadataBackend : public AbstractMetadataBackend {
private:
    std::string path;
    std::shared_ptr<spdlog::logger> log_;

public:
    std::string
    get(const std::string& key) const {
        return static_cast<T const&>(*this).get_impl(key);
    }

    void
    put(const std::string& key, const std::string& val) {
        static_cast<T&>(*this).put_impl(key, val);
    }

    void
    put_no_exist(const std::string& key, const std::string& val) {
        static_cast<T&>(*this).put_no_exist_impl(key, val);
    }

    void
    remove(const std::string& key) {
        static_cast<T&>(*this).remove_impl(key);
    }

    bool
    exists(const std::string& key) {
        return static_cast<T&>(*this).exists_impl(key);
    }

    void
    update(const std::string& old_key, const std::string& new_key,
           const std::string& val) {
        static_cast<T&>(*this).update_impl(old_key, new_key, val);
    }

    off_t
    increase_size(const std::string& key, size_t size, off_t offset,
                  bool append) {
        return static_cast<T&>(*this).increase_size_impl(key, size, offset,
                                                         append);
    }

    void
    decrease_size(const std::string& key, size_t size) {
        static_cast<T&>(*this).decrease_size_impl(key, size);
    }

    std::vector<std::pair<std::string, bool>>
    get_dirents(const std::string& dir) const {
        return static_cast<T const&>(*this).get_dirents_impl(dir);
    }

    std::vector<std::tuple<std::string, bool, size_t, time_t>>
    get_dirents_extended(const std::string& dir) const {
        return static_cast<T const&>(*this).get_dirents_extended_impl(dir);
    }

    void
    iterate_all() const {
        static_cast<T const&>(*this).iterate_all_impl();
    }
};

} // namespace gkfs::metadata

#endif // GEKKOFS_METADATA_BACKEND_HPP
