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

#ifndef IO_COMMANDS_HPP
#define IO_COMMANDS_HPP

// forward declare CLI::App
namespace CLI {
struct App;
}

void
mkdir_init(CLI::App& app);

void
open_init(CLI::App& app);

void
opendir_init(CLI::App& app);

void
read_init(CLI::App& app);

void
pread_init(CLI::App& app);

void
readv_init(CLI::App& app);

void
preadv_init(CLI::App& app);

void
readdir_init(CLI::App& app);

void
rmdir_init(CLI::App& app);

void
stat_init(CLI::App& app);

void
write_init(CLI::App& app);

void
pwrite_init(CLI::App& app);

void
writev_init(CLI::App& app);

void
pwritev_init(CLI::App& app);

#ifdef STATX_TYPE
void
statx_init(CLI::App& app);
#endif

void
lseek_init(CLI::App& app);

void
write_validate_init(CLI::App& app);

void
directory_validate_init(CLI::App& app);

void
write_random_init(CLI::App& app);

void
truncate_init(CLI::App& app);

void
access_init(CLI::App& app);

void
statfs_init(CLI::App& app);

// UTIL
void
file_compare_init(CLI::App& app);
void
chdir_init(CLI::App& app);

void
getcwd_validate_init(CLI::App& app);

void
symlink_init(CLI::App& app);

void
unlink_init(CLI::App& app);

void
dup_validate_init(CLI::App& app);

void
syscall_coverage_init(CLI::App& app);

void
rename_init(CLI::App& app);


#endif // IO_COMMANDS_HPP
