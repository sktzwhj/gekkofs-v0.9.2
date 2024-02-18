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
 * @brief Definitions for the file handle abstraction layer used for the backend
 * file system.
 */
#ifndef GEKKOFS_DAEMON_FILE_HANDLE_HPP
#define GEKKOFS_DAEMON_FILE_HANDLE_HPP

#include <daemon/backend/data/data_module.hpp>

extern "C" {
#include <unistd.h>
}

namespace gkfs::data {

/**
 * @brief File handle class to encapsulate a file descriptor, allowing RAII
 * closing of the file descriptor.
 */
class FileHandle {

private:
    constexpr static const int init_value{-1}; ///< initial file descriptor

    int fd_{init_value}; ///< file descriptor
    std::string path_{}; ///< chunk file path

public:
    FileHandle() = default;

    explicit FileHandle(int fd, const std::string& path) noexcept : fd_(fd) {}

    FileHandle(FileHandle&& rhs) = default;

    FileHandle(const FileHandle& other) = delete;

    FileHandle&
    operator=(FileHandle&& rhs) = default;

    FileHandle&
    operator=(const FileHandle& other) = delete;

    explicit operator bool() const noexcept {
        return valid();
    }

    bool
    operator!() const noexcept {
        return !valid();
    }

    /**
     * @brief Checks for valid file descriptor value.
     * @return boolean if valid file descriptor
     */
    [[nodiscard]] bool
    valid() const noexcept {
        return fd_ != init_value;
    }

    /**
     * @brief Retusn the file descriptor value used in this file handle
     * operation.
     * @return file descriptor value
     */
    [[nodiscard]] int
    native() const noexcept {
        return fd_;
    }

    /**
     * @brief Closes file descriptor and resets it to initial value
     * @return boolean if file descriptor was successfully closed
     */
    bool
    close() noexcept {
        if(fd_ != init_value) {
            if(::close(fd_) < 0) {
                GKFS_DATA_MOD->log()->warn(
                        "{}() Failed to close file descriptor '{}' path '{}' errno '{}'",
                        __func__, fd_, path_, ::strerror(errno));
                return false;
            }
        }
        fd_ = init_value;
        return true;
    }

    /**
     * @brief Destructor implicitly closes the internal file descriptor.
     */
    ~FileHandle() {
        if(fd_ != init_value)
            close();
    }
};

} // namespace gkfs::data


#endif // GEKKOFS_DAEMON_FILE_HANDLE_HPP
