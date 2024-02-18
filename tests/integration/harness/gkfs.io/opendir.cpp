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

/* C includes */
#include <sys/types.h>
#include <dirent.h>

using json = nlohmann::json;

struct opendir_options {
    bool verbose{};
    std::string dirname;

    REFL_DECL_STRUCT(opendir_options, REFL_DECL_MEMBER(bool, verbose),
                     REFL_DECL_MEMBER(std::string, dirname));
};

struct opendir_output {
    ::DIR* dirp;
    int errnum;

    REFL_DECL_STRUCT(opendir_output, REFL_DECL_MEMBER(::DIR*, dirp),
                     REFL_DECL_MEMBER(int, errnum));
};

void
to_json(json& record, const opendir_output& out) {
    record = serialize(out);
}

void
opendir_exec(const opendir_options& opts) {

    ::DIR* dirp = ::opendir(opts.dirname.c_str());

    if(opts.verbose) {
        fmt::print("opendir(name=\"{}\") = {}, errno: {} [{}]\n", opts.dirname,
                   fmt::ptr(dirp), errno, ::strerror(errno));
        return;
    }

    json j = opendir_output{dirp, errno};
    fmt::print("{}\n", j.dump(2));
}

void
opendir_init(CLI::App& app) {
    // Create the option and subcommand objects
    auto opts = std::make_shared<opendir_options>();
    auto* cmd = app.add_subcommand("opendir",
                                   "Execute the opendir() glibc function");

    // Add options to cmd, binding them to opts
    cmd->add_flag("-v,--verbose", opts->verbose,
                  "Produce human readable output");

    cmd->add_option("dirname", opts->dirname, "Directory name")
            ->required()
            ->type_name("");

    cmd->callback([opts]() { opendir_exec(*opts); });
}
