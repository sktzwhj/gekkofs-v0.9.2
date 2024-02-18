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

#@pytest.mark.xfail(reason="invalid errno returned on success")
def test_symlink(gkfs_daemon, gkfs_client):
    """Testing different path resolution capabilities: symlinks"""
    pid = os.getpid().__str__()
    mountdir = gkfs_daemon.mountdir
    extdir = "/tmp/" + pid + "ext.tmp"
    ext_linkdir = "/tmp/" + pid + "link.tmp"
    intdir = mountdir / "int"

    # Just clean if it exists, due to a failed test

    ret = gkfs_client.rmdir(extdir)
    try:
        os.unlink(ext_linkdir) # it is external
    except Exception as e:
        pass

    ret = gkfs_client.mkdir(
        extdir,
        stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    ret = gkfs_client.mkdir(
        intdir,
        stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    #Test external symlink

    ret = gkfs_client.symlink(extdir, ext_linkdir)
    assert ret.retval == 0

    ret = gkfs_client.getcwd_validate(str(extdir) + "/../"+pid+"link.tmp")
    assert ret.path == str(extdir)
    assert ret.retval == 0

    ret = gkfs_client.getcwd_validate(str(intdir) + "/../../../../../../../../../../../../../../../../../../../.."+str(extdir) + "/../"+ pid +"link.tmp/../"+ pid +"link.tmp/../../../../../../../../../../../../../../../../../../../../.."  + str(intdir))
    assert ret.path == str(intdir)
    assert ret.retval == 0

    # Teardown

    # Clean external link
    os.unlink(ext_linkdir)

    ret = gkfs_client.rmdir(extdir)
    assert ret.retval == 0

    #  Clean internal dir
    ret = gkfs_client.rmdir(intdir)
    assert ret.retval == 0

    return
