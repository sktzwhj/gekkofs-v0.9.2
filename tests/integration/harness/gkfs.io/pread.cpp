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

struct pread_options {
    bool verbose{};
    std::string pathname;
    ::size_t count;
    ::size_t offset;

    REFL_DECL_STRUCT(pread_options, REFL_DECL_MEMBER(bool, verbose),
                     REFL_DECL_MEMBER(std::string, pathname),
                     REFL_DECL_MEMBER(::size_t, count),
                     REFL_DECL_MEMBER(::size_t, offset));
};

struct pread_output {
    ::ssize_t retval;
    io::buffer buf;
    int errnum;

    REFL_DECL_STRUCT(pread_output, REFL_DECL_MEMBER(::size_t, retval),
                     REFL_DECL_MEMBER(void*, buf),
                     REFL_DECL_MEMBER(int, errnum));
};

void
to_json(json& record, const pread_output& out) {
    record = serialize(out);
}

void
pread_exec(const pread_options& opts) {

    auto fd = ::open(opts.pathname.c_str(), O_RDONLY);

    if(fd == -1) {
        if(opts.verbose) {
            fmt::print(
                    "pread(pathname=\"{}\", count={}, offset={}) = {}, errno: {} [{}]\n",
                    opts.pathname, opts.count, opts.offset, fd, errno,
                    ::strerror(errno));
            return;
        }

        json out = pread_output{fd, nullptr, errno};
        fmt::print("{}\n", out.dump(2));

        return;
    }

    io::buffer buf(opts.count);

    int rv = ::pread(fd, buf.data(), opts.count, opts.offset);

    if(opts.verbose) {
        fmt::print(
                "pread(pathname=\"{}\", count={}, offset={}) = {}, errno: {} [{}]\n",
                opts.pathname, opts.count, opts.offset, rv, errno,
                ::strerror(errno));
        return;
    }

    json out = pread_output{rv, (rv != -1 ? buf : nullptr), errno};
    fmt::print("{}\n", out.dump(2));
}

void
pread_init(CLI::App& app) {

    // Create the option and subcommand objects
    auto opts = std::make_shared<pread_options>();
    auto* cmd = app.add_subcommand("pread", "Execute the pread() system call");

    // Add options to cmd, binding them to opts
    cmd->add_flag("-v,--verbose", opts->verbose,
                  "Produce human readable output");

    cmd->add_option("pathname", opts->pathname, "Directory name")
            ->required()
            ->type_name("");

    cmd->add_option("count", opts->count, "Number of bytes to read")
            ->required()
            ->type_name("");

    cmd->add_option("offset", opts->offset, "Offset to read")
            ->required()
            ->type_name("");

    cmd->callback([opts]() { pread_exec(*opts); });
}
