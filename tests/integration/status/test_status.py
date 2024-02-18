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
from datetime import datetime

nonexisting = "nonexisting"




#@pytest.mark.xfail(reason="invalid errno returned on success")
def test_statx(gkfs_daemon, gkfs_client):
    """Test several statx commands"""
    topdir = gkfs_daemon.mountdir / "top"
    longer = Path(topdir.parent, topdir.name + "_plus")
    dir_a  = topdir / "dir_a"
    dir_b  = topdir / "dir_b"
    file_a = topdir / "file_a"
    subdir_a  = dir_a / "subdir_a"

    # creation timestamp
    # it is only checked if mtime and ctime are greater than
    # the creation timestamp due to inprecesion between test and daemon
    ts = int(datetime.timestamp(datetime.now()))

    # create topdir
    ret = gkfs_client.mkdir(
            topdir,
            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    # test statx on existing dir
    ret = gkfs_client.statx(0, topdir, 0, 0)

    assert ret.retval == 0
    assert stat.S_ISDIR(ret.statbuf.stx_mode)

    ret = gkfs_client.open(file_a,
                           os.O_CREAT,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval != -1


    # test statx on existing file
    ret = gkfs_client.statx(0, file_a, 0, 0)

    assert ret.retval == 0
    assert (stat.S_ISDIR(ret.statbuf.stx_mode)==0)
    assert (ret.statbuf.stx_size == 0)


    buf = b'42'
    ret = gkfs_client.write(file_a, buf, 2)

    assert ret.retval == 2

    # test statx on existing file
    ret = gkfs_client.statx(0, file_a, 0, 0)

    assert ret.retval == 0
    assert (ret.statbuf.stx_size == 2)

    # if greater zero, it is activated in config.hpp
    if ret.statbuf.stx_mtime.tv_sec > 0:
        assert (ret.statbuf.stx_mtime.tv_sec > ts)

    if ret.statbuf.stx_ctime.tv_sec > 0:
        assert (ret.statbuf.stx_ctime.tv_sec > ts)


    return


