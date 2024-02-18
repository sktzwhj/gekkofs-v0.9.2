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

#ifndef GEKKOFS_INTERCEPT_HPP
#define GEKKOFS_INTERCEPT_HPP

namespace gkfs::preload {

int
internal_hook_guard_wrapper(long syscall_number, long arg0, long arg1,
                            long arg2, long arg3, long arg4, long arg5,
                            long* syscall_return_value);

int
hook_guard_wrapper(long syscall_number, long arg0, long arg1, long arg2,
                   long arg3, long arg4, long arg5, long* syscall_return_value);

void
start_self_interception();

void
start_interception();

void
stop_interception();

} // namespace gkfs::preload

#endif
