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

/* C++ includes */
#include <CLI/CLI.hpp>
#include <nlohmann/json.hpp>
#include <memory>
#include <fmt/format.h>
#include <commands.hpp>
#include <reflection.hpp>
#include <serialize.hpp>
#include <binary_buffer.hpp>

/* C includes */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using json = nlohmann::json;

struct rename_options {
    bool verbose{};
    std::string pathname;
    std::string pathname2;

    REFL_DECL_STRUCT(rename_options, REFL_DECL_MEMBER(bool, verbose),
                     REFL_DECL_MEMBER(std::string, pathname),
                     REFL_DECL_MEMBER(std::string, pathname2));
};

struct rename_output {
    ::off_t retval;
    int errnum;

    REFL_DECL_STRUCT(rename_output, REFL_DECL_MEMBER(::off_t, retval),
                     REFL_DECL_MEMBER(int, errnum));
};

void
to_json(json& record, const rename_output& out) {
    record = serialize(out);
}


void
rename_exec(const rename_options& opts) {

    int fd = ::rename(opts.pathname.c_str(), opts.pathname2.c_str());

    if(fd == -1) {
        if(opts.verbose) {
            fmt::print("rename(pathname=\"{}\", pathname2=\"{}\") = {}, errno: {} [{}]\n",
                       opts.pathname, opts.pathname2, fd, errno, ::strerror(errno));
            return;
        }
    }

    json out = rename_output{fd, errno};
        fmt::print("{}\n", out.dump(2));

        return;
}

void
rename_init(CLI::App& app) {

    // Create the option and subcommand objects
    auto opts = std::make_shared<rename_options>();
    auto* cmd = app.add_subcommand("rename", "Execute the rename() system call");

    // Add options to cmd, binding them to opts
    cmd->add_flag("-v,--verbose", opts->verbose,
                  "Produce human writeable output");

    cmd->add_option("pathname", opts->pathname, "old name")
            ->required()
            ->type_name("");

    cmd->add_option("pathname2", opts->pathname2, "new name")
            ->required()
            ->type_name("");

    cmd->callback([opts]() { rename_exec(*opts); });
}
