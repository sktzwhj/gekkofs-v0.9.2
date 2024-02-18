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
#include <random>
#include <climits>

/* C includes */
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>

#ifndef CHAR_BIT
#define CHAR_BIT 8
#endif
constexpr int seed = 42;

using json = nlohmann::json;

struct write_random_options {
    bool verbose{};
    std::string pathname{};
    ::size_t count{};

    REFL_DECL_STRUCT(write_random_options, REFL_DECL_MEMBER(bool, verbose),
                     REFL_DECL_MEMBER(std::string, pathname),
                     REFL_DECL_MEMBER(::size_t, count));
};

struct write_random_output {
    ::ssize_t retval;
    int errnum;

    REFL_DECL_STRUCT(write_random_output, REFL_DECL_MEMBER(::size_t, retval),
                     REFL_DECL_MEMBER(int, errnum));
};

void
to_json(json& record, const write_random_output& out) {
    record = serialize(out);
}

/**
 * Writes `count` random bytes to file
 * @param opts
 */
void
write_random_exec(const write_random_options& opts) {

    auto fd = ::open(opts.pathname.c_str(), O_WRONLY);

    if(fd == -1) {
        if(opts.verbose) {
            fmt::print("open(pathname=\"{}\", count={}) = {}, errno: {} [{}]\n",
                       opts.pathname, opts.count, fd, errno, ::strerror(errno));
            return;
        }

        json out = write_random_output{fd, errno};
        fmt::print("{}\n", out.dump(2));
        return;
    }
    // random number generator with seed
    std::independent_bits_engine<std::default_random_engine, CHAR_BIT,
                                 unsigned char>
            engine{seed};
    // create buffer for opts.count
    std::vector<uint8_t> data(opts.count);
    std::generate(begin(data), end(data), std::ref(engine));
    // pass data to buffer
    io::buffer buf(data);

    int rv = ::write(fd, buf.data(), opts.count);

    if(opts.verbose) {
        fmt::print("write(pathname=\"{}\", count={}) = {}, errno: {} [{}]\n",
                   opts.pathname, opts.count, rv, errno, ::strerror(errno));
        return;
    }

    json out = write_random_output{rv, errno};
    fmt::print("{}\n", out.dump(2));
}

void
write_random_init(CLI::App& app) {

    // Create the option and subcommand objects
    auto opts = std::make_shared<write_random_options>();
    auto* cmd = app.add_subcommand("write_random",
                                   "Execute the write() system call ");

    // Add options to cmd, binding them to opts
    cmd->add_flag("-v,--verbose", opts->verbose,
                  "Produce human writeable output");

    cmd->add_option("pathname", opts->pathname, "File name")
            ->required()
            ->type_name("");

    cmd->add_option("count", opts->count, "Number of random bytes to write")
            ->required()
            ->type_name("");

    cmd->callback([opts]() { write_random_exec(*opts); });
}
