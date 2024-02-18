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
#include <chrono>

using namespace std;

using ns = chrono::nanoseconds;
using get_time = chrono::steady_clock;

int main(int argc, char* argv[]) {

    auto filen = atoi(argv[1]);

//    cout << mkdir("/tmp/mountdir/bla", 0775) << endl;
//    auto buf = "BUFFERINO2";
//    struct stat attr;
//    cout << creat("/tmp/mountdir/creat.txt", 0666) << endl;
//    cout << creat("/tmp/mountdir/#test-dir.0/mdtest_tree.0/file.mdtest.0000000.0000000005", 0666) << endl;
//    cout << stat("/tmp/mountdir/creat.txt", &attr) << endl;
//    cout << unlink("/tmp/mountdir/creat.txt") << endl;


    auto start_t = get_time::now();
    int fd;
    for (int i = 0; i < filen; ++i) {
        string p = "/tmp/mountdir/file" + to_string(i);
        fd = creat(p.c_str(), 0666);
        if (i % 25000 == 0)
            cout << i << " files processed." << endl;
        close(fd);
    }

    auto end_t = get_time::now();
    auto diff = end_t - start_t;

    auto diff_count = chrono::duration_cast<ns>(diff).count();

    cout << diff_count << "ns\t" << (diff_count) / 1000000. << "ms" << endl;
    cout << filen / ((diff_count) / 1000000000.) << " files per second" << endl;

    return 0;

}