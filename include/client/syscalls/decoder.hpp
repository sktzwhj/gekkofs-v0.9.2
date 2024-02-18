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

#ifndef GKFS_SYSCALLS_DECODER_HPP
#define GKFS_SYSCALLS_DECODER_HPP

#include <client/syscalls/syscall.hpp>
#include <client/syscalls/args.hpp>
#include <client/syscalls/rets.hpp>

namespace gkfs::syscall {

namespace detail {

/** a RAII saver/restorer of errno values */
struct errno_saver {
    errno_saver(int errnum) : saved_errno_(errnum) {}

    ~errno_saver() {
        errno = saved_errno_;
    }

    const int saved_errno_;
};

} // namespace detail

template <typename FmtBuffer>
inline void
decode(FmtBuffer& buffer, const long syscall_number,
       const long argv[MAX_ARGS]) {

    detail::errno_saver _(errno);

    const auto sc = lookup_by_number(syscall_number, argv);

    fmt::format_to(buffer, "{}(", sc.name());

    for(int i = 0; i < sc.num_args(); ++i) {
        const auto& arg = sc.args().at(i);

        arg.formatter<FmtBuffer>()(buffer, {arg.name(), argv[i]});

        if(i < sc.num_args() - 1) {
            fmt::format_to(buffer, ", ");
        }
    }

    fmt::format_to(buffer, ") = ?");
}

template <typename FmtBuffer>
inline void
decode(FmtBuffer& buffer, const long syscall_number, const long argv[MAX_ARGS],
       const long result) {

    detail::errno_saver _(errno);

    const auto sc = lookup_by_number(syscall_number, argv);

    fmt::format_to(buffer, "{}(", sc.name());

    for(int i = 0; i < sc.num_args(); ++i) {
        const auto& arg = sc.args().at(i);

        arg.formatter<FmtBuffer>()(buffer, {arg.name(), argv[i]});

        if(i < sc.num_args() - 1) {
            fmt::format_to(buffer, ", ");
        }
    }

    if(never_returns(syscall_number)) {
        fmt::format_to(buffer, ") = ?");
        return;
    }

    if(error_code(result) != 0) {
        fmt::format_to(buffer, ") = {} {} ({})", static_cast<int>(-1),
                       errno_name(-result), errno_message(-result));
        return;
    }

    fmt::format_to(buffer, ") = ");
    const auto& ret = sc.return_type();
    ret.formatter<FmtBuffer>()(buffer, result);
}

} // namespace gkfs::syscall

#endif // GKFS_SYSCALLS_DECODER_HPP