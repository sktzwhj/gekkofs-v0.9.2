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

#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <limits>

using namespace std;

int main(int argc, char* argv[]) {

    string mountdir = "/tmp/mountdir";
    string f = mountdir + "/file";
    int fd;

    fd = open(f.c_str(), O_WRONLY | O_CREAT, 0777);
    if(fd < 0){
        cerr << "Error opening file (write): " << strerror(errno) << endl;
        return -1;
    }
    off_t pos = static_cast<off_t>(numeric_limits<int>::max()) + 1;

    off_t ret = lseek(fd, pos, SEEK_SET);
    if(ret == -1) {
        cerr << "Error seeking file: " << strerror(errno) << endl;
        return -1;
    }

    if(ret != pos) {
        cerr << "Error seeking file: unexpected returned position " << ret << endl;
        return -1;
    }

    if(close(fd) != 0){
        cerr << "Error closing file" << endl;
        return -1;
    }

    /* Remove test file */
    ret = remove(f.c_str());
    if(ret != 0){
        cerr << "Error removing file: " << strerror(errno) << endl;
        return -1;
    };
}
