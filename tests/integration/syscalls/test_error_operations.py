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

from sre_parse import State
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


def test_open_error(gkfs_daemon, gkfs_client):

    file = gkfs_daemon.mountdir / "file"
    file2 = gkfs_daemon.mountdir / "file2"
    file3 = gkfs_daemon.mountdir / "file3"
    
    flags = [os.O_PATH, os.O_CREAT | os.O_DIRECTORY]
    # create a file in gekkofs

    for flag in flags:
        ret = gkfs_client.open(file,
                            flag,
                            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        assert ret.retval == -1
        assert ret.errno == errno.ENOTSUP


    # Create file and recreate
    ret = gkfs_client.open(file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    assert ret.retval == 10000
    
    ret = gkfs_client.open(file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    assert ret.retval == -1
    assert ret.errno == errno.EEXIST


    # Create file and recreate
    ret = gkfs_client.open(file2, os.O_CREAT | os.O_WRONLY)
    assert ret.retval == 10000
    # Undefined in man 
    ret = gkfs_client.open(file2, os.O_CREAT | os.O_WRONLY)
    assert ret.retval == 10000
   
    # RDWR
    ret = gkfs_client.open(file2, os.O_RDWR)
    assert ret.retval == 10000

    # RD
    ret = gkfs_client.open(file2, os.O_RDONLY)
    assert ret.retval == 10000
   
    # Truncate the file
    ret = gkfs_client.open(file2, os.O_TRUNC | os.O_WRONLY)
    assert ret.retval == 10000

    # Open unexistent file
    ret = gkfs_client.open(file3, os.O_WRONLY)
    assert ret.retval == -1
    assert ret.errno == errno.ENOENT

    ret = gkfs_client.open(file3, os.O_CREAT | stat.S_IFSOCK | os.O_EXCL | os.O_WRONLY)
    assert ret.retval == 10000

def test_access_error(gkfs_daemon, gkfs_client):

    file = gkfs_daemon.mountdir / "file"
    file2 = gkfs_daemon.mountdir / "file2"

    ret = gkfs_client.open(file, os.O_CREAT | os.O_WRONLY)
    assert ret.retval == 10000

    # Access, flags are not being used
    ret = gkfs_client.access(file2, os.R_OK)
    assert ret.retval == -1
    assert ret.errno == errno.ENOENT

    ret = gkfs_client.access(file, os.R_OK)
    assert ret.retval != -1

def test_stat_error(gkfs_daemon, gkfs_client):
    # Stat non existing file
    file = gkfs_daemon.mountdir / "file"

    ret = gkfs_client.stat(file)
    assert ret.retval == -1
    assert ret.errno == errno.ENOENT

      # test statx on existing file
    ret = gkfs_client.statx(0, file, 0, 0)
    assert ret.retval == -1
    assert ret.errno == errno.ENOENT

def test_statfs(gkfs_daemon, gkfs_client):
    # Statfs check most of the outputs

    ret = gkfs_client.statfs(gkfs_daemon.mountdir)  
    assert ret.retval == 0
    assert ret.statfsbuf.f_type == 0
    assert ret.statfsbuf.f_bsize != 0
    assert ret.statfsbuf.f_blocks != 0
    assert ret.statfsbuf.f_bfree != 0
    assert ret.statfsbuf.f_bavail != 0
    assert ret.statfsbuf.f_files == 0
    assert ret.statfsbuf.f_ffree == 0
 

def test_check_parents(gkfs_daemon, gkfs_client):
    file = gkfs_daemon.mountdir / "dir" / "file"
    file2 = gkfs_daemon.mountdir / "file2"
    file3 = gkfs_daemon.mountdir / "file2" / "file3"

    # Parent directory does not exist
    ret = gkfs_client.open(file, os.O_CREAT | os.O_WRONLY)
    assert ret.retval == -1
    assert ret.errno == errno.ENOENT

    # Create file
    ret = gkfs_client.open(file2, os.O_CREAT | os.O_WRONLY)
    assert ret.retval == 10000

    # Create file over a file
    ret = gkfs_client.open(file3, os.O_CREAT | os.O_WRONLY)
    assert ret.retval == -1
    assert ret.errno == errno.ENOTDIR


def test_dup(gkfs_daemon, gkfs_client):
    file = gkfs_daemon.mountdir / "file"

    ret = gkfs_client.open(file, os.O_CREAT | os.O_WRONLY)
    assert ret.retval == 10000

    ret = gkfs_client.dup_validate(file)
    assert ret.retval == 0
    assert ret.errno == 0

