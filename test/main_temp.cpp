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

//
// Created by evie on 1/16/18.
//

#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <iostream>
#include <cstring>
#include <sys/statfs.h>

using namespace std;

int main(int argc, char* argv[]) {

//    auto path = "/tmp/mountdir/test";
    auto path = "/tmp/testing/test";

    auto fd = creat(path, 0667);
    struct stat mystat{};
    fstat(fd, &mystat);
    struct statfs sfs{};
    auto ret = statfs(path, &sfs);
    cout << ret << " errno:" << errno << endl;












//    char buf[] = "lefthyblubber";
//    char buf1[] = "rebbulbyhtfellefthyblubber";
//
//    auto fd = creat(path, 0677);
//    auto fd_dup = dup2(fd,33);
//    struct stat mystat{};
//    fstat(fd, &mystat);
//    auto nw = write(fd, &buf, strlen(buf));
//    fstat(fd_dup, &mystat);
//    close(fd);
//    auto nw_dup = pwrite(fd_dup, &buf1, strlen(buf1), 0);
//    fstat(fd_dup, &mystat);
//    close(fd_dup);
//    nw_dup = pwrite(fd_dup, &buf1, strlen(buf1), 0);

    return 0;
}