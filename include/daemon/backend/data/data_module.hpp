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
/**
 * @brief Declaration for GekkoFS data module object that is run once per daemon
 * instance.
 */

#ifndef GEKKOFS_DAEMON_DATA_LOGGING_HPP
#define GEKKOFS_DAEMON_DATA_LOGGING_HPP

#include <spdlog/spdlog.h>

namespace gkfs::data {

/**
 * @brief The data module class providing the data backend for the daemon as a
 * singleton.
 */
class DataModule {

private:
    DataModule() = default;

    std::shared_ptr<spdlog::logger> log_; ///< Logging instance for data backend

public:
    static constexpr const char* LOGGER_NAME = "DataModule";

    static DataModule*
    getInstance() {
        static DataModule instance;
        return &instance;
    }

    DataModule(DataModule const&) = delete;

    void
    operator=(DataModule const&) = delete;

    /**
     * @brief Returns the data module log handle.
     * @return Pointer to the spdlog instance
     */
    [[nodiscard]] const std::shared_ptr<spdlog::logger>&
    log() const;

    /**
     * @brief Attaches a logging instance to the data module.
     * @param log spdlog shared pointer instance
     */
    void
    log(const std::shared_ptr<spdlog::logger>& log);
};

#define GKFS_DATA_MOD                                                          \
    (static_cast<gkfs::data::DataModule*>(                                     \
            gkfs::data::DataModule::getInstance())) ///< macro to access the
                                                    ///< DataModule singleton
                                                    ///< across the daemon

} // namespace gkfs::data

#endif // GEKKOFS_DAEMON_DATA_LOGGING_HPP
