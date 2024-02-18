/*
  Copyright 2018-2024, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2024, Johannes Gutenberg Universitaet Mainz, Germany

  This software was partially supported by the
  EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).

  This software was partially supported by the
  ADA-FS project under the SPPEXA project funded by the DFG.

  This file is part of GekkoFS' POSIX interface.

  GekkoFS' POSIX interface is free software: you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public License as
  published by the Free Software Foundation, either version 3 of the License,
  or (at your option) any later version.

  GekkoFS' POSIX interface is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with GekkoFS' POSIX interface.  If not, see
  <https://www.gnu.org/licenses/>.

  SPDX-License-Identifier: LGPL-3.0-or-later
*/

#ifndef LIBGKFS_UTILS_MAKE_ARRAY_HPP
#define LIBGKFS_UTILS_MAKE_ARRAY_HPP

#include <array>

namespace gkfs::utils {

template <typename... T>
constexpr auto
make_array(T&&... values) -> std::array<
        typename std::decay<typename std::common_type<T...>::type>::type,
        sizeof...(T)> {
    return std::array<
            typename std::decay<typename std::common_type<T...>::type>::type,
            sizeof...(T)>{std::forward<T>(values)...};
}

} // namespace gkfs::utils

#endif // LIBGKFS_UTILS_MAKE_ARRAY_HPP
