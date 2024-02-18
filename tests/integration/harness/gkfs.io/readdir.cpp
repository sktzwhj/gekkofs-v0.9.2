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
#include <dirent.h>
#include <unistd.h>

using json = nlohmann::json;

struct readdir_options {
    bool verbose{};
    std::string pathname;
    ::size_t count;

    REFL_DECL_STRUCT(readdir_options, REFL_DECL_MEMBER(bool, verbose),
                     REFL_DECL_MEMBER(std::string, pathname),
                     REFL_DECL_MEMBER(::size_t, count));
};

struct readdir_output {
    std::vector<struct ::dirent> dirents;
    int errnum;

    REFL_DECL_STRUCT(readdir_output,
                     REFL_DECL_MEMBER(std::vector<struct ::dirent>, dirents),
                     REFL_DECL_MEMBER(int, errnum));
};

void
to_json(json& record, const readdir_output& out) {
    record = serialize(out);
}

void
readdir_exec(const readdir_options& opts) {

    ::DIR* dirp = ::opendir(opts.pathname.c_str());

    if(dirp == NULL) {
        if(opts.verbose) {
            fmt::print("readdir(pathname=\"{}\") = {}, errno: {} [{}]\n",
                       opts.pathname, "NULL", errno, ::strerror(errno));
            return;
        }

        json out = readdir_output{{}, errno};
        fmt::print("{}\n", out.dump(2));

        return;
    }

    io::buffer buf(opts.count);

    std::vector<struct ::dirent> entries;
    struct ::dirent* entry;

    while((entry = ::readdir(dirp)) != NULL) {
        entries.push_back(*entry);
    }

    if(opts.verbose) {
        fmt::print("readdir(pathname=\"{}\") = [\n{}],\nerrno: {} [{}]\n",
                   opts.pathname, fmt::join(entries, ",\n"), errno,
                   ::strerror(errno));
        return;
    }

    json out = readdir_output{entries, errno};
    fmt::print("{}\n", out.dump(2));
}

void
readdir_init(CLI::App& app) {

    // Create the option and subcommand objects
    auto opts = std::make_shared<readdir_options>();
    auto* cmd =
            app.add_subcommand("readdir", "Execute the readdir() system call");

    // Add options to cmd, binding them to opts
    cmd->add_flag("-v,--verbose", opts->verbose,
                  "Produce human readable output");

    cmd->add_option("pathname", opts->pathname, "Directory name")
            ->required()
            ->type_name("");

    cmd->callback([opts]() { readdir_exec(*opts); });
}
