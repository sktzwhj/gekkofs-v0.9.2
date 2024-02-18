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
#include <unistd.h>

using json = nlohmann::json;

struct unlink_options {
    bool verbose{};
    std::string pathname;

    REFL_DECL_STRUCT(unlink_options, REFL_DECL_MEMBER(bool, verbose),
                     REFL_DECL_MEMBER(std::string, pathname));
};

struct unlink_output {
    int retval;
    int errnum;

    REFL_DECL_STRUCT(unlink_output, REFL_DECL_MEMBER(int, retval),
                     REFL_DECL_MEMBER(int, errnum));
};

void
to_json(json& record, const unlink_output& out) {
    record = serialize(out);
}

void
unlink_exec(const unlink_options& opts) {

    auto fd = ::unlink(opts.pathname.c_str());

    if(opts.verbose) {
        fmt::print("unlink(pathname=\"{}\") = {}, errno: {} [{}]\n",
                   opts.pathname, errno, ::strerror(errno));
        return;
    }

    json out = unlink_output{fd, errno};
    fmt::print("{}\n", out.dump(2));

    return;
}

void
unlink_init(CLI::App& app) {

    // Create the option and subcommand objects
    auto opts = std::make_shared<unlink_options>();
    auto* cmd = app.add_subcommand("unlink", "Execute the unlink() system call");

    // Add options to cmd, binding them to opts
    cmd->add_flag("-v,--verbose", opts->verbose,
                  "Produce human readable output");

    cmd->add_option("pathname", opts->pathname, "File name")
            ->required()
            ->type_name("");

    cmd->callback([opts]() { unlink_exec(*opts); });
}
