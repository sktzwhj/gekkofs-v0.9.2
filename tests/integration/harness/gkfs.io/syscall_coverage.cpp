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
#include <sys/xattr.h>

// Syscall numbers
#include <sys/syscall.h>

using json = nlohmann::json;

struct syscall_coverage_options {
    bool verbose{};
    std::string pathname;


    REFL_DECL_STRUCT(syscall_coverage_options, REFL_DECL_MEMBER(bool, verbose),
                     REFL_DECL_MEMBER(std::string, pathname));
};

struct syscall_coverage_output {
    int retval;
    int errnum;
    std::string syscall;

    REFL_DECL_STRUCT(syscall_coverage_output, REFL_DECL_MEMBER(int, retval),
                     REFL_DECL_MEMBER(int, errnum),
                     REFL_DECL_MEMBER(std::string, syscall));
};

void
to_json(json& record, const syscall_coverage_output& out) {
    record = serialize(out);
}

void
output(const std::string syscall, const int ret,
       const syscall_coverage_options& opts) {
    if(opts.verbose) {
        fmt::print(
                "syscall_coverage[{}](pathname=\"{}\") = {}, errno: {} [{}]\n",
                syscall, opts.pathname, ret, errno, ::strerror(errno));
    }

    json out = syscall_coverage_output{ret, errno, syscall};
    fmt::print("{}\n", out.dump(2));
}

/*
 *  system calls
 *  that are being tested.
 */
void
syscall_coverage_exec(const syscall_coverage_options& opts) {

    int fd = ::open(opts.pathname.c_str(), O_RDWR);

    if(fd == -1) {
        output("open", fd, opts);
        return;
    }

    // create external
    int fdext = ::open("tmpfile", O_RDWR | O_CREAT, 0666);

    //  faccessat Internal

    auto rv = ::faccessat(AT_FDCWD, opts.pathname.c_str(), F_OK, 0);

    if(rv < 0) {
        output("faccessat", rv, opts);
        return;
    }

    // faccessat External

    rv = ::faccessat(AT_FDCWD, "/tmp", F_OK, 0);

    if(rv < 0) {
        output("faccessat, external", rv, opts);
        return;
    }

    // lstat
    struct stat st;
    rv = ::lstat(opts.pathname.c_str(), &st);
    if(rv < 0) {
        output("lstat", rv, opts);
        return;
    }

    rv = ::lstat("tmpfile", &st);
    if(rv < 0) {
        output("lstat", rv, opts);
        return;
    }

    // pwrite external
    rv = ::pwrite(fdext, "test", 4, 0);
    if(rv < 0) {
        output("pwrite", rv, opts);
        return;
    }

    // pread external
    char bufext[4];
    rv = ::pread(fdext, bufext, 4, 0);
    if(rv < 0) {
        output("pread", rv, opts);
        return;
    }

    // lseek external
    rv = ::lseek(fdext, 0, SEEK_SET);
    if(rv < 0) {
        output("lseek", rv, opts);
        return;
    }

    // ftruncate external
    rv = ::ftruncate(fdext, 0);
    if(rv < 0) {
        output("ftruncate", rv, opts);
        return;
    }

    // truncate exterlan
    rv = ::truncate("tmpfile", 0);
    if(rv < 0) {
        output("truncate", rv, opts);
        return;
    }


    // dup external
    int fdext2 = ::dup(fdext);
    if(fdext2 < 0) {
        output("dup", fdext2, opts);
        return;
    }

    // dup2 external
    int ffdext3 = 0;
    rv = ::dup2(fdext, ffdext3);
    if(rv < 0) {
        output("dup2", rv, opts);
        return;
    }


    // fchmod internal
    rv = ::fchmod(fd, 0777);
    if(errno != ENOTSUP) {
        output("fchmod", rv, opts);
        return;
    }

    // fchmod external
    rv = ::fchmod(fdext, 0777);
    if(rv < 0) {
        output("fchmod", rv, opts);
        return;
    }

    // fchmodat internal
    rv = ::fchmodat(AT_FDCWD, opts.pathname.c_str(), 0777, 0);
    if(errno != ENOTSUP) {
        output("fchmodat", rv, opts);
        return;
    }
    // fchmodat external
    rv = ::fchmodat(AT_FDCWD, "tmpfile", 0777, 0);
    if(rv < 0) {
        output("fchmodat, external", rv, opts);
        return;
    }

    // dup3 internal
    rv = ::dup3(fd, 0, 0);
    if(errno != ENOTSUP) {
        output("dup3", rv, opts);
        return;
    }

    // dup3 external
    int ffdext4 = 0;
    rv = ::dup3(fdext, ffdext4, 0);
    if(rv < 0) {
        output("dup3", rv, opts);
        return;
    }


    // fcntl
    rv = ::fcntl(fd, F_GETFD);
    if(rv < 0) {
        output("fcntl, F_GETFD", rv, opts);
        return;
    }

    rv = ::fcntl(fd, F_GETFL);
    if(rv < 0 || rv != O_RDWR) {
        output("fcntl, F_GETFL", rv, opts);
        return;
    }


    rv = ::fcntl(fd, F_SETFD, 0);
    if(rv < 0) {
        output("fcntl, F_SETFD", rv, opts);
        return;
    }

    rv = ::fcntl(fd, F_SETFL, 0);
    if(errno != ENOTSUP) {
        output("fcntl, F_SETFL", rv, opts);
        return;
    }

    rv = ::fcntl(fd, F_DUPFD, 0);
    if(rv < 0) {
        output("fcntl, F_DUPFD", rv, opts);
        return;
    }

    rv = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
    if(rv < 0) {
        output("fcntl, F_DUPFD_CLOEXEC", rv, opts);
        return;
    }

    // Fstatfs internal

    struct statfs stfs;
    rv = ::fstatfs(fd, &stfs);
    if(rv < 0) {
        output("fstatfs", rv, opts);
        return;
    }

    // Fstatfs external
    rv = ::fstatfs(fdext, &stfs);
    if(rv < 0) {
        output("fstatfs", rv, opts);
        return;
    }

    // fsync

    rv = ::fsync(fd);
    if(rv < 0) {
        output("fsync", rv, opts);
        return;
    }

    // getxattr

    char buf[1024];
    rv = ::getxattr(opts.pathname.c_str(), "user.test", buf, sizeof(buf));
    if(errno != ENOTSUP) {
        output("getxattr", rv, opts);
        return;
    }

    // readlinkat
    rv = ::readlinkat(AT_FDCWD, opts.pathname.c_str(), buf, sizeof(buf));
    if(errno != ENOTSUP) {
        output("readlinkat", rv, opts);
        return;
    }

    // chdir internal error
    rv = ::chdir(opts.pathname.c_str());
    if(errno != ENOTDIR) {
        output("chdir", rv, opts);
        return;
    }

    // chdir internal error
    std::string nonexist = opts.pathname+"x2";
    rv = ::chdir(nonexist.c_str());
    if(rv >= 0) {
        output("chdir", rv, opts);
        return;
    }

    // fchdir
    auto fddir = ::open(".", O_RDONLY);
    if(fddir < 0) {
        output("fchdir", fddir, opts);
        return;
    }

    rv = ::fchdir(fddir);
    if(rv < 0) {
        output("fchdir", rv, opts);
        return;
    }

    // ftruncate
    rv = ::ftruncate(fd, 0);
    if(rv < 0) {
        output("ftruncate", rv, opts);
        return;
    }

    // fchdir internal file
    rv = ::fchdir(fd);
    if(errno != EBADF) {
        output("fchdir", rv, opts);
        return;
    }


    // fchdir directory from opts.pathname
    auto fd2 = ::open(
            opts.pathname.substr(0, opts.pathname.find_last_of("/")).c_str(),
            O_RDONLY);
    if(fd2 < 0) {
        output("fchdir", fd2, opts);
        return;
    }
    rv = ::fchdir(fd2);
    if(rv < 0) {
        output("fchdir", rv, opts);
        return;
    }

    std::string pid = std::to_string(getpid());
    std::string path1 = "/tmp/"+pid+"test_rename";
    std::string path2 = "/tmp/"+pid+"test_rename2";

    // renameat external
    auto fdtmp = ::open(path1.c_str(), O_CREAT | O_WRONLY, 0644);
    ::close(fdtmp);

    rv = ::renameat(AT_FDCWD, path1.c_str(), AT_FDCWD,
                    opts.pathname.c_str());
    if(errno != ENOTSUP) {
        output("renameat_ext_to_int", rv, opts);
        return;
    }

    rv = ::renameat(AT_FDCWD, path1.c_str(), AT_FDCWD, path2.c_str());
    if(rv < 0) {
        output("renameat_ext_to_ext", rv, opts);
        return;
    }

    // open with O_APPEND
    std::string path_append = "/tmp/" + pid + "test_append";
    auto fd_append = ::open(path1.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    if(fd_append < 0) {
        output("open with O_APPEND", fd_append, opts);
        return;
    }
    rv = ::write(fd_append, "testappend", 10);
    if(rv < 0) {
        output("open with O_APPEND", rv, opts);
        return;
    }
    ::close(fd_append);

    // sys_open
    rv = ::syscall(SYS_open, opts.pathname.c_str(), O_RDONLY, 0);
    if(rv < 0) {
        output("sys_open", rv, opts);
        return;
    }

    // sys_creat
    rv = ::syscall(SYS_creat, opts.pathname.c_str(), 0777);
    if(rv < 0) {
        output("sys_creat", rv, opts);
        return;
    }

    // sys_unlinkat
    rv = ::syscall(SYS_unlinkat, AT_FDCWD, opts.pathname.c_str(), 0);
    if(rv < 0) {
        output("sys_unlinkat", rv, opts);
        return;
    }

    // sys_mkdirat
    std::string path = opts.pathname + "path";
    rv = ::syscall(SYS_mkdirat, AT_FDCWD, opts.pathname.c_str(), 0777);
    if(rv < 0) {
        output("sys_mkdirat", rv, opts);
        return;
    }

    // SYS_chmod
    rv = ::syscall(SYS_chmod, opts.pathname.c_str(), 0777);
    if(errno != ENOTSUP) {
        output("sys_chmod", rv, opts);
        return;
    }

    // hook_faccessat coverage
    rv = ::syscall(SYS_faccessat, AT_FDCWD, opts.pathname.c_str(), F_OK, 0);
    if(rv < 0) {
        output("sys_faccessat", rv, opts);
        return;
    }

    rv = ::syscall(SYS_faccessat, AT_FDCWD, "/tmp", F_OK, 0);
    if(rv < 0) {
        output("sys_faccessat", rv, opts);
        return;
    }


    rv = 0;
    errno = 0;
    auto syscall = "ALLOK";
    json out = syscall_coverage_output{(int) rv, errno, syscall};
    fmt::print("{}\n", out.dump(2));
    return;
}

void
syscall_coverage_init(CLI::App& app) {

    // Create the option and subcommand objects
    auto opts = std::make_shared<syscall_coverage_options>();
    auto* cmd =
            app.add_subcommand("syscall_coverage", "Execute severals syscalls");

    // Add options to cmd, binding them to opts
    cmd->add_flag("-v,--verbose", opts->verbose,
                  "Produce human writeable output");

    cmd->add_option("pathname", opts->pathname, "File name")
            ->required()
            ->type_name("");

    cmd->callback([opts]() { syscall_coverage_exec(*opts); });
}
