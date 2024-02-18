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
#include <sys/stat.h>
#include <array>
using namespace std;

int main(int argc, char* argv[]) {

    string mountdir = "/tmp/mountdir";
    string f = mountdir + "/file";
    std::array<unsigned char, 1024> buffIn {'i'};
    std::array<unsigned char, 1024> buffOut {'\0'};
    unsigned int size_after_trunc = 2;
    int fd;
    int ret;
    struct stat st;

    fd = open(f.c_str(), O_WRONLY | O_CREAT, 0777);
    if(fd < 0){
        cerr << "Error opening file (write)" << endl;
        return -1;
    }
    auto nw = write(fd, buffIn.data(), buffIn.size());
    if(nw != buffIn.size()){
        cerr << "Error writing file" << endl;
        return -1;
    }

    if(close(fd) != 0){
        cerr << "Error closing file" << endl;
        return -1;
    }

    ret = truncate(f.c_str(), size_after_trunc);
    if(ret != 0){
        cerr << "Error truncating file: " << strerror(errno) << endl;
        return -1;
    };

    /* Check file size */
    ret = stat(f.c_str(), &st);
    if(ret != 0){
        cerr << "Error stating file: " << strerror(errno) << endl;
        return -1;
    };

    if(st.st_size != size_after_trunc){
        cerr << "Wrong file size after truncation: " << st.st_size << endl;
        return -1;
    }


    /* Read the file back */

    fd = open(f.c_str(), O_RDONLY);
    if(fd < 0){
        cerr << "Error opening file (read)" << endl;
        return -1;
    }

    auto nr = read(fd, buffOut.data(), buffOut.size());
    if(nr != size_after_trunc){
        cerr << "[Error] read more then file size: " << nr << endl;
        return -1;
    }

    ret = close(fd);
    if(ret != 0){
        cerr << "Error closing file: " << strerror(errno) << endl;
        return -1;
    };

    /* Remove test file */
    ret = remove(f.c_str());
    if(ret != 0){
        cerr << "Error removing file: " << strerror(errno) << endl;
        return -1;
    };
}
