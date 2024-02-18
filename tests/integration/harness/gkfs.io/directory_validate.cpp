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
#include <libgen.h>

using json = nlohmann::json;

// Creates a file in the path, and does a readdir comparing the count size (may be accumulated with previous operations)
struct directory_validate_options {
    bool verbose{};
    std::string pathname;
    ::size_t count;

    REFL_DECL_STRUCT(directory_validate_options, REFL_DECL_MEMBER(bool, verbose),
                     REFL_DECL_MEMBER(std::string, pathname),
                     REFL_DECL_MEMBER(::size_t, count));
};

struct directory_validate_output {
    int retval;
    int errnum;

    REFL_DECL_STRUCT(directory_validate_output, REFL_DECL_MEMBER(int, retval),
                     REFL_DECL_MEMBER(int, errnum));
};

void
to_json(json& record, const directory_validate_output& out) {
    record = serialize(out);
}


/**
 * returns the number of elements existing in the path
 * @param opts
 * @param dir Path checked
 * @returns The number of elements in the directory
 */
int 
number_of_elements(const directory_validate_options& opts, const std::string dir) {
    int num_elements = 0;

    ::DIR* dirp = ::opendir(dir.c_str());

    if(dirp == NULL) {
        if(opts.verbose) {
            fmt::print("readdir(pathname=\"{}\") = {}, errno: {} [{}]\n",
                       dir, "NULL", errno, ::strerror(errno));
            return -3;
        }

    std::cout << "Error create directory" << std::endl;
        json out = directory_validate_output{-3, errno};
        fmt::print("{}\n", out.dump(2));

        return -3;
    }

    struct ::dirent* entry;

    while((entry = ::readdir(dirp)) != NULL) {
        num_elements++;
    }

    return num_elements;
}


/**
 * Creates `count` files, with a suffix starting at the number of elements existing in the path
 * @param opts
 * @param path Path where the file is created
 * @param count Number of files to create
 * @returns The number of elements created plus the number of elements already in the directory (calculated, not checked)
 */
int
create_n_files (const directory_validate_options& opts, const std::string path, int count) {

    // Read Directory and get number of entries
    int num_elements = number_of_elements(opts, path);

    for (int i = num_elements; i < count+num_elements; i++) {   
        std::string filename = path+"/file_auto_"+std::to_string(i);
        int fd = ::creat(filename.c_str(), S_IRWXU);
        if(fd == -1) {
            if(opts.verbose) {
                fmt::print(
                        "directory_validate(pathname=\"{}\", count={}) = {}, errno: {} [{}]\n",
                        filename, i, fd, errno, ::strerror(errno));
                return -2;
            }
            json out = directory_validate_output{-2, errno};
            fmt::print("{}\n", out.dump(2));

            return -2;
        }

        ::close(fd);
    }
    return num_elements+count;
}

/**
 * Creates `count` files, and returns the number of elements in the path
 * If count == 0, the path is the filename to be created. Parent directory is checked
 * @param opts
 */
void
directory_validate_exec(const directory_validate_options& opts) {

    if (opts.count == 0)  {
        int fd = ::creat(opts.pathname.c_str(), S_IRWXU);
    
        if(fd == -1) {
            if(opts.verbose) {
                fmt::print(
                        "directory_validate(pathname=\"{}\", count={}) = {}, errno: {} [{}]\n",
                        opts.pathname, opts.count, fd, errno, ::strerror(errno));
                return;
            }
            json out = directory_validate_output{-2, errno};
            fmt::print("{}\n", out.dump(2));

            return;
        }

        ::close(fd);
    }
    else {
        int created = create_n_files(opts, opts.pathname, opts.count);
        if (created <= 0) return;
    }

    // Do a readdir
    std::string dir = opts.pathname;

    if (opts.count == 0)
        dir = ::dirname((char*)opts.pathname.c_str());
    
    auto num_elements = number_of_elements(opts, dir); 

    if(opts.verbose) {
        fmt::print("readdir(pathname=\"{}\") = [\n{}],\nerrno: {} [{}]\n",
                   opts.pathname, num_elements, errno,
                   ::strerror(errno));
        return;
    }
    
    errno = 0;
    json out = directory_validate_output{num_elements, errno};
    fmt::print("{}\n", out.dump(2));
    return;
    
}

void
directory_validate_init(CLI::App& app) {

    // Create the option and subcommand objects
    auto opts = std::make_shared<directory_validate_options>();
    auto* cmd = app.add_subcommand(
            "directory_validate",
            "Create count files in the directory and execute a direntry system call and returns the number of elements");

    // Add options to cmd, binding them to opts
    cmd->add_flag("-v,--verbose", opts->verbose,
                  "Produce human writeable output");

    cmd->add_option("pathname", opts->pathname, "directory to check or filename to create (if count is 0), elements will be checked in the parent dir")
            ->required()
            ->type_name("");

    cmd->add_option("count", opts->count, "Number of files to create. If 0, it creates only the entry in the pathname.")
            ->required()
            ->type_name("");

    cmd->callback([opts]() { directory_validate_exec(*opts); });
}
