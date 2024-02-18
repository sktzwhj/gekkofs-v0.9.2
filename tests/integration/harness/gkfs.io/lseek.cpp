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

struct lseek_options {
    bool verbose{};
    std::string pathname;
    ::off_t offset;
    int whence;

    REFL_DECL_STRUCT(lseek_options, REFL_DECL_MEMBER(bool, verbose),
                     REFL_DECL_MEMBER(std::string, pathname),
                     REFL_DECL_MEMBER(::off_t, offset),
                     REFL_DECL_MEMBER(int, whence));
};

struct lseek_output {
    ::off_t retval;
    int errnum;

    REFL_DECL_STRUCT(lseek_output, REFL_DECL_MEMBER(::off_t, retval),
                     REFL_DECL_MEMBER(int, errnum));
};

void
to_json(json& record, const lseek_output& out) {
    record = serialize(out);
}

std::string
whence2str(int whence) {
    switch(whence) {
        case SEEK_SET:
            return "SEEK_SET";
        case SEEK_CUR:
            return "SEEK_CUR";
        case SEEK_END:
            return "SEEK_END";
        default:
            return "UNKNOWN";
    }
    return "UNKNOWN";
}

void
lseek_exec(const lseek_options& opts) {

    int fd = ::open(opts.pathname.c_str(), O_RDONLY);

    if(fd == -1) {
        if(opts.verbose) {
            fmt::print("open(pathname=\"{}\") = {}, errno: {} [{}]\n",
                       opts.pathname, fd, errno, ::strerror(errno));
            return;
        }

        json out = lseek_output{fd, errno};
        fmt::print("{}\n", out.dump(2));

        return;
    }

    int rv = ::lseek(fd, opts.offset, opts.whence);

    if(opts.verbose) {
        fmt::print(
                "lseek(pathname=\"{}\", offset='{}', whence='{}') = {}, errno: {} [{}]\n",
                opts.pathname, opts.offset, whence2str(opts.whence), rv, errno,
                ::strerror(errno));
        return;
    }

    json out = lseek_output{rv, errno};
    fmt::print("{}\n", out.dump(2));
}

void
lseek_init(CLI::App& app) {

    // Create the option and subcommand objects
    auto opts = std::make_shared<lseek_options>();
    auto* cmd = app.add_subcommand("lseek", "Execute the lseek() system call");

    // Add options to cmd, binding them to opts
    cmd->add_flag("-v,--verbose", opts->verbose,
                  "Produce human writeable output");

    cmd->add_option("pathname", opts->pathname, "Directory name")
            ->required()
            ->type_name("");

    cmd->add_option("offset", opts->offset, "offset used")
            ->required()
            ->type_name("");

    cmd->add_option("whence", opts->whence, "Whence the action is done")
            ->required()
            ->type_name("");

    cmd->callback([opts]() { lseek_exec(*opts); });
}
