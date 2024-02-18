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

#include <daemon/backend/metadata/metadata_module.hpp>

namespace gkfs::metadata {

const std::shared_ptr<spdlog::logger>&
MetadataModule::log() const {
    return log_;
}

void
MetadataModule::log(const std::shared_ptr<spdlog::logger>& log) {
    MetadataModule::log_ = log;
}

const std::map<uint16_t, size_t>&
MetadataModule::append_offset_reserve() const {
    return append_offset_reserve_;
}

void
MetadataModule::append_offset_reserve_put(uint16_t merge_id, size_t offset) {
    std::lock_guard<std::mutex> lock(append_offset_reserve_mutex_);
    append_offset_reserve_[merge_id] = offset;
}

size_t
MetadataModule::append_offset_reserve_get_and_erase(uint16_t merge_id) {
    std::lock_guard<std::mutex> lock(append_offset_reserve_mutex_);
    auto out = append_offset_reserve_.at(merge_id);
    append_offset_reserve_.erase(merge_id);
    return out;
}

} // namespace gkfs::metadata
