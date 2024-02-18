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

#ifndef GEKKOFS_DB_EXCEPTIONS_HPP
#define GEKKOFS_DB_EXCEPTIONS_HPP

#include <string>
#include <stdexcept>

namespace gkfs::metadata {

class DBException : public std::runtime_error {
public:
    explicit DBException(const std::string& s) : std::runtime_error(s){};
};

class NotFoundException : public DBException {
public:
    explicit NotFoundException(const std::string& s) : DBException(s){};
};

class ExistsException : public DBException {
public:
    explicit ExistsException(const std::string& s) : DBException(s){};
};

} // namespace gkfs::metadata

#endif // GEKKOFS_DB_EXCEPTIONS_HPP
