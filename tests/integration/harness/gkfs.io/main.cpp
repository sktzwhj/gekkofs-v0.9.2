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

//#include <io/command.hpp>

#include <cstdlib>
#include <string>
#include <CLI/CLI.hpp>
#include <commands.hpp>

void
init_commands(CLI::App& app) {
    open_init(app);
    opendir_init(app);
    mkdir_init(app);
    read_init(app);
    pread_init(app);
    readv_init(app);
    preadv_init(app);
    readdir_init(app);
    rmdir_init(app);
    stat_init(app);
    write_init(app);
    pwrite_init(app);
    writev_init(app);
    pwritev_init(app);
#ifdef STATX_TYPE
    statx_init(app);
#endif
    lseek_init(app);
    write_validate_init(app);
    directory_validate_init(app);
    write_random_init(app);
    truncate_init(app);
    access_init(app);
    statfs_init(app);
    // utils
    file_compare_init(app);
    chdir_init(app);
    getcwd_validate_init(app);
    symlink_init(app);
    unlink_init(app);
    dup_validate_init(app);
    syscall_coverage_init(app);
    rename_init(app);
}


int
main(int argc, char* argv[]) {

    CLI::App app{"GekkoFS I/O client"};
    app.require_subcommand(1);
    app.get_formatter()->label("REQUIRED", "");
    app.set_help_all_flag("--help-all", "Expand all help");
    init_commands(app);
    CLI11_PARSE(app, argc, argv);

    return EXIT_SUCCESS;
}
