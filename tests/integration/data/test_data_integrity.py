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
import sys
import pytest
import string
import random
from harness.logger import logger


def generate_random_data(size):
    return ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(size)])


def check_write(gkfs_client, i, file):
        buf = bytes(generate_random_data(i), sys.stdout.encoding)
        
        ret = gkfs_client.write(file, buf, i)

        assert ret.retval == i
        ret = gkfs_client.stat(file)

        assert ret.retval == 0
        assert (ret.statbuf.st_size == i)

        ret = gkfs_client.read(file, i)
        assert ret.retval== i
        assert ret.buf == buf

#@pytest.mark.xfail(reason="invalid errno returned on success")
def test_data_integrity(gkfs_daemon, gkfs_client):
    """Test several data write-read commands and check that the data is correct"""
    topdir = gkfs_daemon.mountdir / "top"
    file_a = topdir / "file_a"

    # create topdir
    ret = gkfs_client.mkdir(
            topdir,
            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    # test stat on existing dir
    ret = gkfs_client.stat(topdir)

    assert ret.retval == 0
    assert (stat.S_ISDIR(ret.statbuf.st_mode))

    ret = gkfs_client.open(file_a,
                   os.O_CREAT,
                   stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval != -1


    # test stat on existing file
    ret = gkfs_client.stat(file_a)

    assert ret.retval == 0
    assert (stat.S_ISDIR(ret.statbuf.st_mode)==0)
    assert (ret.statbuf.st_size == 0)

    # Step 1 - small sizes
    
    # Generate writes
    # Read data
    # Compare buffer

    ret = gkfs_client.write_validate(file_a, 1)
    assert ret.retval == 1    

    ret = gkfs_client.write_validate(file_a, 256)
    assert ret.retval == 1  

    ret = gkfs_client.write_validate(file_a, 512)
    assert ret.retval == 1  

    # Step 2 - Compare bigger sizes exceeding typical chunksize and not aligned
    ret = gkfs_client.write_validate(file_a, 128192)
    assert ret.retval == 1

    # < 1 chunk   
    ret = gkfs_client.write_validate(file_a, 400000)
    assert ret.retval == 1

    # > 1 chunk < 2 chunks
    ret = gkfs_client.write_validate(file_a, 600000)
    assert ret.retval == 1

    # > 1 chunk < 2 chunks
    ret = gkfs_client.write_validate(file_a, 900000)
    assert ret.retval == 1

    # > 2 chunks
    ret = gkfs_client.write_validate(file_a, 1100000) 
    assert ret.retval == 1

    # > 4 chunks
    ret = gkfs_client.write_validate(file_a, 2097153) 
    assert ret.retval == 1

