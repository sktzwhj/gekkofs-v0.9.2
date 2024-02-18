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


def test_write(gkfs_daemon, gkfs_client):

    file = gkfs_daemon.mountdir / "file"

    ret = gkfs_client.open(file,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    buf = b'42'
    ret = gkfs_client.write(file, buf, len(buf))

    assert ret.retval == len(buf)  # Return the number of written bytes

    file_append = gkfs_daemon.mountdir / "file_append"

    ret = gkfs_client.open(file_append,
                           os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    str1 = b'Hello'
    str2 = b', World!'
    str3 = b' This is a test.\n'

    ret = gkfs_client.write(file_append, str1, len(str1), True)
    assert ret.retval == len(str1)
    ret = gkfs_client.stat(file_append)
    assert ret.retval == 0
    assert ret.statbuf.st_size == len(str1)

    ret = gkfs_client.write(file_append, str2, len(str2), True)
    assert ret.retval == len(str2)
    ret = gkfs_client.stat(file_append)
    assert ret.retval == 0
    assert ret.statbuf.st_size == (len(str1) + len(str2))

    ret = gkfs_client.write(file_append, str3, len(str3), True)
    assert ret.retval == len(str3)
    ret = gkfs_client.stat(file_append)
    assert ret.retval == 0
    assert ret.statbuf.st_size == (len(str1) + len(str2) + len(str3))


def test_pwrite(gkfs_daemon, gkfs_client):
    file = gkfs_daemon.mountdir / "file"

    ret = gkfs_client.open(file,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    buf = b'42'
    # write at the offset 1024
    ret = gkfs_client.pwrite(file, buf, len(buf), 1024)

    assert ret.retval == len(buf)  # Return the number of written bytes


def test_writev(gkfs_daemon, gkfs_client):
    file = gkfs_daemon.mountdir / "file"

    ret = gkfs_client.open(file,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    buf_0 = b'42'
    buf_1 = b'24'
    ret = gkfs_client.writev(file, buf_0, buf_1, 2)

    assert ret.retval == len(buf_0) + len(buf_1)  # Return the number of written bytes


def test_pwritev(gkfs_daemon, gkfs_client):
    file = gkfs_daemon.mountdir / "file"

    ret = gkfs_client.open(file,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    buf_0 = b'42'
    buf_1 = b'24'
    ret = gkfs_client.pwritev(file, buf_0, buf_1, 2, 1024)

    assert ret.retval == len(buf_0) + len(buf_1)  # Return the number of written bytes
