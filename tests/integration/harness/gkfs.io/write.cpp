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

struct write_options {
    bool verbose{};
    std::string pathname;
    std::string data;
    ::size_t count;
    bool append{false};

    REFL_DECL_STRUCT(write_options, REFL_DECL_MEMBER(bool, verbose),
                     REFL_DECL_MEMBER(std::string, pathname),
                     REFL_DECL_MEMBER(std::string, data),
                     REFL_DECL_MEMBER(::size_t, count));
};

struct write_output {
    ::ssize_t retval;
    int errnum;

    REFL_DECL_STRUCT(write_output, REFL_DECL_MEMBER(::size_t, retval),
                     REFL_DECL_MEMBER(int, errnum));
};

void
to_json(json& record, const write_output& out) {
    record = serialize(out);
}

void
write_exec(const write_options& opts) {
    auto flags = O_WRONLY;
    if(opts.append)
        flags |= O_APPEND;
    auto fd = ::open(opts.pathname.c_str(), flags);

    if(fd == -1) {
        if(opts.verbose) {
            fmt::print(
                    "open(pathname=\"{}\", buf=\"{}\" count={}) = {}, errno: {} [{}]\n",
                    opts.pathname, opts.data, opts.count, fd, errno,
                    ::strerror(errno));
            return;
        }

        json out = write_output{fd, errno};
        fmt::print("{}\n", out.dump(2));

        return;
    }

    io::buffer buf(opts.data);
    auto rv = ::write(fd, buf.data(), opts.count);

    if(opts.verbose) {
        fmt::print("write(pathname=\"{}\", count={}) = {}, errno: {} [{}]\n",
                   opts.pathname, opts.count, rv, errno, ::strerror(errno));
        return;
    }

    json out = write_output{rv, errno};
    fmt::print("{}\n", out.dump(2));
}

void
write_init(CLI::App& app) {

    // Create the option and subcommand objects
    auto opts = std::make_shared<write_options>();
    auto* cmd = app.add_subcommand("write", "Execute the write() system call");

    // Add options to cmd, binding them to opts
    cmd->add_flag("-v,--verbose", opts->verbose,
                  "Produce human writeable output");

    cmd->add_option("pathname", opts->pathname, "Directory name")
            ->required()
            ->type_name("");

    cmd->add_option("data", opts->data, "Data to write")
            ->required()
            ->type_name("");

    cmd->add_option("count", opts->count, "Number of bytes to write")
            ->required()
            ->type_name("");

    cmd->add_option("append", opts->append, "Append file")
            ->default_val(false)
            ->type_name("");

    cmd->callback([opts]() { write_exec(*opts); });
}
