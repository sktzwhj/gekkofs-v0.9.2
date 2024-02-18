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

#ifndef GEKKOFS_DAEMON_METADATA_LOGGING_HPP
#define GEKKOFS_DAEMON_METADATA_LOGGING_HPP

#include <spdlog/spdlog.h>
#include <map>

namespace gkfs::metadata {

/**
 * @brief MetadataModule is a singleton class that holds global data structures
 * for all metadata operations.
 */
class MetadataModule {

private:
    MetadataModule() = default;

    std::shared_ptr<spdlog::logger> log_; ///< Metadata logger
    ///< Map to remember and assign offsets to write append operations
    std::map<uint16_t, size_t> append_offset_reserve_{};
    std::mutex append_offset_reserve_mutex_{}; ///< Mutex to protect
                                               ///< append_offset_reserve_

public:
    ///< Logger name
    static constexpr const char* LOGGER_NAME = "MetadataModule";

    /**
     * @brief Get the MetadataModule singleton instance
     * @return MetadataModule instance
     */
    static MetadataModule*
    getInstance() {
        static MetadataModule instance;
        return &instance;
    }

    MetadataModule(MetadataModule const&) = delete;

    void
    operator=(MetadataModule const&) = delete;

    const std::shared_ptr<spdlog::logger>&
    log() const;

    void
    log(const std::shared_ptr<spdlog::logger>& log);

    const std::map<uint16_t, size_t>&
    append_offset_reserve() const;

    /**
     * @brief Inserts entry into append_offset_reserve_
     * @param merge_id Merge ID
     * @param offset Offset to reserve
     */
    void
    append_offset_reserve_put(uint16_t merge_id, size_t offset);

    /**
     * @brief Gets and erases entry from append_offset_reserve_
     * @param merge_id Merge ID
     * @return Offset reserved for merge_id
     * @throws std::out_of_range if merge_id is not in append_offset_reserve_
     */
    size_t
    append_offset_reserve_get_and_erase(uint16_t merge_id);
};

#define GKFS_METADATA_MOD                                                      \
    (static_cast<gkfs::metadata::MetadataModule*>(                             \
            gkfs::metadata::MetadataModule::getInstance()))

} // namespace gkfs::metadata

#endif // GEKKOFS_DAEMON_METADATA_LOGGING_HPP
