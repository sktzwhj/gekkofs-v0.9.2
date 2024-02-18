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

#ifndef IO_BINARY_BUFFER_HPP
#define IO_BINARY_BUFFER_HPP

#include <nlohmann/json.hpp>
#include <utility>
#include <fmt/format.h>


namespace io {

struct buffer {
    buffer(::size_t size = 0) : m_data(size) {}

    buffer(nullptr_t p) {}

    buffer(const std::string& s) {
        m_data.clear();
        std::copy(s.begin(), s.end(), std::back_inserter(m_data));
    }

    buffer(std::vector<uint8_t> data) : m_data(std::move(data)) {}

    bool
    operator==(nullptr_t) const {
        return m_data.empty();
    }

    bool
    operator!=(nullptr_t) const {
        return !m_data.empty();
    }

    auto
    data() {
        return m_data.data();
    }

    auto
    storage() const {
        return m_data;
    }

    std::size_t
    size() const {
        return m_data.size();
    }

    std::vector<uint8_t> m_data;
};

inline void
to_json(nlohmann::json& record, const buffer& out) {

    if(out == nullptr) {
        record = nullptr;
    } else {

        //    record = fmt::format("x{:2x}", fmt::join(out, "x"));
        record = out.storage();
    }
}

} // namespace io

#endif // IO_BINARY_BUFFER_HPP
