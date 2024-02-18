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
from harness.logger import logger

nonexisting = "nonexisting"



def test_unlink(gkfs_daemon, gkfs_client):

    file = gkfs_daemon.mountdir / "file"
    dir = gkfs_daemon.mountdir / "dir"
    

    # Delete an unexistent file
    ret = gkfs_client.unlink(file)
    assert ret.retval == -1
    assert ret.errno == errno.ENOENT





    # create a file in gekkofs
    ret = gkfs_client.open(file,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    # write a buffer we know
    buf = b'42'
    ret = gkfs_client.write(file, buf, len(buf))
    assert ret.retval == len(buf) # Return the number of written bytes

  

    ret = gkfs_client.unlink(file) # Remove renamed file (success)
    assert ret.retval == 0        

    ret = gkfs_client.stat(file) # file does not exist
    assert ret.retval != 0 
    assert ret.errno == errno.ENOENT  

    ret = gkfs_client.mkdir(dir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO) # Create a directory
    assert ret.retval == 0

    ret = gkfs_client.unlink(dir)
    assert ret.retval == -1
    assert ret.errno == errno.EISDIR
    

    # create a file in gekkofs
    ret = gkfs_client.open(file,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    # > 4 chunks
    ret = gkfs_client.write_validate(file, 2097153) 
    assert ret.retval == 1

    ret = gkfs_client.unlink(file) # Remove renamed file (extra chunks, success)
    assert ret.retval == 0
