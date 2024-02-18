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

#include <fmt/format.h>
#include "helpers.hpp"

namespace fs = std::filesystem;

namespace helpers {


temporary_directory::temporary_directory()
    : dirname_(fs::temp_directory_path() / random_string(16)) {

    std::error_code ec;
    fs::create_directory(dirname_, ec);

    if(ec) {
        throw std::runtime_error(fmt::format("Error creating temporary "
                                             "directory: {}",
                                             ec.message()));
    }
}

temporary_directory::~temporary_directory() {
    fs::remove_all(dirname_);
}

fs::path
temporary_directory::dirname() const {
    return dirname_;
}

} // namespace helpers