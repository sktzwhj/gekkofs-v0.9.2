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

#include "helpers.hpp"

namespace fs = std::filesystem;

namespace helpers {

temporary_file::temporary_file(fs::path filename)
    : filename_(std::move(filename)), ofs_(filename_) {}

temporary_file::temporary_file(fs::path filename, const std::string_view& text)
    : filename_(std::move(filename)), ofs_(filename_) {
    write(text);
}

temporary_file::~temporary_file() {
    ofs_.close();
    fs::remove(filename_);
}

void
temporary_file::write(const std::string_view& text) {
    ofs_ << text;
    ofs_.flush();
}

void
temporary_file::write(const std::vector<char>& data) {
    for(const auto n : data) {
        ofs_ << n;
    }
    ofs_.flush();
}

fs::path
temporary_file::filename() const {
    return filename_;
}

std::size_t
temporary_file::size() const {
    return fs::file_size(filename_);
}

} // namespace helpers