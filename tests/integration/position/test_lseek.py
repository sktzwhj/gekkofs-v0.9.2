################################################################################
# Copyright 2018-2024, Barcelona Supercomputing Center (BSC), Spain            #
# Copyright 2015-2024, Johannes Gutenberg Universitaet Mainz, Germany          #
#                                                                              #
# This software was partially supported by the                                 #
# EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).    #
#                                                                              #
# This software was partially supported by the                                 #
# ADA-FS project under the SPPEXA project funded by the DFG.                   #
#                                                                              #
# This file is part of GekkoFS.                                                #
#                                                                              #
# GekkoFS is free software: you can redistribute it and/or modify              #
# it under the terms of the GNU General Public License as published by         #
# the Free Software Foundation, either version 3 of the License, or            #
# (at your option) any later version.                                          #
#                                                                              #
# GekkoFS is distributed in the hope that it will be useful,                   #
# but WITHOUT ANY WARRANTY; without even the implied warranty of               #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                #
# GNU General Public License for more details.                                 #
#                                                                              #
# You should have received a copy of the GNU General Public License            #
# along with GekkoFS.  If not, see <https://www.gnu.org/licenses/>.            #
#                                                                              #
# SPDX-License-Identifier: GPL-3.0-or-later                                    #
################################################################################

import harness
from pathlib import Path
import errno
import stat
import os
import ctypes
import sh
import sys
import pytest
from harness.logger import logger

nonexisting = "nonexisting"



#TESTING LSEEK


#SEEK_SET: int
#SEEK_CUR: int
#SEEK_END: int

# 1. LSeek on empty file
# 2. LSeek on non-empty file



#@pytest.mark.xfail(reason="invalid errno returned on success")
def test_lseek(gkfs_daemon, gkfs_client):
    """Test several statx commands"""
    topdir = gkfs_daemon.mountdir / "top"
    longer = Path(topdir.parent, topdir.name + "_plus")
    file_a = topdir / "file_a"

    # create topdir
    ret = gkfs_client.mkdir(
            topdir,
            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

     # test stat on existing dir
    ret = gkfs_client.stat(topdir)

    assert ret.retval == 0
    assert stat.S_ISDIR(ret.statbuf.st_mode)

    ret = gkfs_client.open(file_a,
                           os.O_CREAT,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval != -1

    # LSeek on empty file

    ret = gkfs_client.lseek(file_a, 0, os.SEEK_SET)

    assert ret.retval == 0

    ret = gkfs_client.lseek(file_a, -1, os.SEEK_SET)
    assert ret.retval == -1
    assert ret.errno == 22

    # We can go further an empty file

    ret = gkfs_client.lseek(file_a, 5, os.SEEK_SET)
    assert ret.retval == 5

    ret = gkfs_client.lseek(file_a, 2, os.SEEK_CUR)
    assert ret.retval == 2
    
    # Size needs to be 0 
    ret = gkfs_client.stat(file_a)

    assert ret.retval == 0
    assert (stat.S_ISDIR(ret.statbuf.st_mode)==0)
    assert (ret.statbuf.st_size == 0)


    # next commands write at the end of the file (pos0), as the filedescriptor is not shared
    buf = b'42'
    ret = gkfs_client.write(file_a, buf, 2)

    assert ret.retval == 2

    # Size should be 2
    ret = gkfs_client.stat(file_a)

    assert ret.retval == 0
    assert (stat.S_ISDIR(ret.statbuf.st_mode)==0)
    assert (ret.statbuf.st_size == 2)


    ret = gkfs_client.lseek(file_a, 0, os.SEEK_END)
    assert ret.retval == 2                      #FAILS

    ret = gkfs_client.lseek(file_a, -2, os.SEEK_END)
    assert ret.retval == 0                     #FAILS

    ret = gkfs_client.lseek(file_a, -3, os.SEEK_END)
    assert ret.retval == -1                     #FAILS
    assert ret.errno == 22 

    ret = gkfs_client.lseek(file_a, 0, os.SEEK_DATA)
    assert ret.retval == -1
    assert ret.errno == errno.EINVAL

    ret = gkfs_client.lseek(file_a, 0, os.SEEK_HOLE)
    assert ret.retval == -1
    assert ret.errno == errno.EINVAL

    ret = gkfs_client.lseek(file_a, 0, 666)
    assert ret.retval == -1
    assert ret.errno == errno.EINVAL



  
    return


