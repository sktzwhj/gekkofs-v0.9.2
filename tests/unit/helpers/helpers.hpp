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

#ifndef GKFS_TESTS_HELPERS
#define GKFS_TESTS_HELPERS

#include <filesystem>
#include <fstream>
#include <vector>

namespace fs = std::filesystem;

namespace helpers {

/**
 * Generate a random string of length `length`.
 *
 * @param length The length of the random string.
 * @return The generated string.
 */
std::string
random_string(std::size_t length);


/**
 * Read the contents of `filename` returning them into `str`.
 *
 * @param filename The filename to read data from.
 * @param str The string where data will be copied.
 */
inline void
load_string_file(const fs::path& filename, std::string& str) {

    std::ifstream file;
    file.exceptions(std::ios_base::failbit | std::ios_base::badbit);
    file.open(filename, std::ios_base::binary);

    auto sz = static_cast<std::size_t>(fs::file_size(filename));
    str.resize(sz, '\0');
    file.read(&str[0], static_cast<std::streamsize>(sz));
}

/**
 * A temporary directory with RAII removal
 */
struct temporary_directory {

    /**
     * Create a temporary directory with a random dirname.
     * The directory is created at a location suitable for temporary files.
     */
    temporary_directory();

    /**
     * Remove a temporary directory and all its contents.
     */
    ~temporary_directory();

    /**
     * Return the path of the created temporary directory.
     *
     * @return The path of the created directory.
     */
    [[nodiscard]] fs::path
    dirname() const;

    fs::path dirname_;
};

/**
 * A temporary file with RAII removal
 */
struct temporary_file {

    /**
     * Create an empty temporary file with `filename` as its name.
     *
     * @param filename The desired filename for the file.
     */
    explicit temporary_file(fs::path filename);

    /**
     * Create a temporary file with `filename` as its name and `text` as its
     * contents.
     *
     * @param filename The desired filename for the file.
     * @param text The text to be used for contents.
     */
    temporary_file(fs::path filename, const std::string_view& text);

    /**
     * Destroy and remove a temporary file.
     */
    ~temporary_file();

    /**
     * Write text data to the temporary file.
     *
     * @param text The text to write.
     */
    void
    write(const std::string_view& text);

    /**
     * Write binary data to the temporary file.
     *
     * @param text The binary data to write.
     */
    void
    write(const std::vector<char>& data);

    /**
     * Return the `filename` for the temporary file.
     *
     * @return The temporary file's `filename`.
     */
    fs::path
    filename() const;

    /**
     * Return the `size` for the temporary file.
     *
     * @return The temporary file's `size`.
     */
    std::size_t
    size() const;

    fs::path filename_;
    std::ofstream ofs_;
};

} // namespace helpers

#endif // GKFS_TESTS_HELPERS