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


#@pytest.mark.xfail(reason="invalid errno returned on success")
def test_mkdir(gkfs_daemon, gkfs_client):
    """Create a new directory in the FS's root"""

    topdir = gkfs_daemon.mountdir / "top"
    longer = Path(topdir.parent, topdir.name + "_plus")
    dir_a  = topdir / "dir_a"
    dir_b  = topdir / "dir_b"
    file_a = topdir / "file_a"
    subdir_a  = dir_a / "subdir_a"

    # create topdir
    ret = gkfs_client.mkdir(
            topdir,
            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    # test stat on existing dir
    ret = gkfs_client.stat(topdir)

    assert ret.retval == 0
    assert stat.S_ISDIR(ret.statbuf.st_mode)

    # open topdir
    ret = gkfs_client.open(topdir, os.O_DIRECTORY)
    assert ret.retval != -1


    # read and write should be impossible on directories
    ret = gkfs_client.read(topdir, 1)

    assert ret.buf is None
    assert ret.retval == -1
    assert ret.errno == errno.EISDIR

    # buf = bytes('42', sys.stdout.encoding)
    # print(buf.hex())
    buf = b'42'
    ret = gkfs_client.write(topdir, buf, 1)

    assert ret.retval == -1
    assert ret.errno == errno.EISDIR


    # read top directory that is empty
    ret = gkfs_client.opendir(topdir)

    assert ret.dirp is not None

    ret = gkfs_client.readdir(topdir)

    # XXX: This might change in the future if we add '.' and '..'
    assert len(ret.dirents) == 0

    # close directory
    # TODO: disabled for now because we have no way to keep DIR* alive
    # between gkfs.io executions
    # ret = gkfs_client.opendir(XXX)


    # populate top directory
    for d in [dir_a, dir_b]:
        ret = gkfs_client.mkdir(
                d,
                stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        assert ret.retval == 0

    ret = gkfs_client.open(file_a,
                           os.O_CREAT,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval != -1

    ret = gkfs_client.readdir(gkfs_daemon.mountdir)

    # XXX: This might change in the future if we add '.' and '..'
    assert len(ret.dirents) == 1
    assert ret.dirents[0].d_name == 'top'
    assert ret.dirents[0].d_type == 4 # DT_DIR

    expected = [
        ( dir_a.name,  4 ), # DT_DIR
        ( dir_b.name,  4 ),
        ( file_a.name, 8 ) # DT_REG
    ]

    ret = gkfs_client.readdir(topdir)
    assert len(ret.dirents) == len(expected)

    for d,e in zip(ret.dirents, expected):
        assert d.d_name == e[0]
        assert d.d_type == e[1]

    # remove file using rmdir should produce an error
    ret = gkfs_client.rmdir(file_a)
    assert ret.retval == -1
    assert ret.errno == errno.ENOTDIR

    # create a directory with the same prefix as topdir but longer name
    ret = gkfs_client.mkdir(
            longer,
            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    expected = [
        ( topdir.name,  4 ), # DT_DIR
        ( longer.name,  4 ), # DT_DIR
    ]

    ret = gkfs_client.readdir(gkfs_daemon.mountdir)
    assert len(ret.dirents) == len(expected)

    for d,e in zip(ret.dirents, expected):
        assert d.d_name == e[0]
        assert d.d_type == e[1]

    # create 2nd level subdir and check it's not included in readdir()
    ret = gkfs_client.mkdir(
            subdir_a,
            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    expected = [
        ( topdir.name,  4 ), # DT_DIR
        ( longer.name,  4 ), # DT_DIR
    ]

    ret = gkfs_client.readdir(gkfs_daemon.mountdir)
    assert len(ret.dirents) == len(expected)

    for d,e in zip(ret.dirents, expected):
        assert d.d_name == e[0]
        assert d.d_type == e[1]

    expected = [
        ( subdir_a.name,  4 ), # DT_DIR
    ]

    ret = gkfs_client.readdir(dir_a)

    assert len(ret.dirents) == len(expected)

    for d,e in zip(ret.dirents, expected):
        assert d.d_name == e[0]
        assert d.d_type == e[1]

    # Try to open a file as a dir
    ret = gkfs_client.opendir(file_a)
    assert ret.dirp is None
    assert ret.errno == errno.ENOTDIR

    # Try to remove a non empty dir
    ret = gkfs_client.rmdir(topdir)
    assert ret.retval == -1
    assert ret.errno == errno.ENOTEMPTY

    return

#@pytest.mark.xfail(reason="invalid errno returned on success")
def test_finedir(gkfs_daemon, gkfs_client):
    """Tests several corner cases for directories scan"""

    topdir = gkfs_daemon.mountdir / "finetop"
    file_a  = topdir / "file_"
    
    
    # create topdir
    ret = gkfs_client.mkdir(
            topdir,
            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    ret = gkfs_client.readdir(topdir)

    # XXX: This might change in the future if we add '.' and '..'
    assert len(ret.dirents) == 0

    # populate top directory

    for files in range (1,4):
        ret = gkfs_client.directory_validate(
                topdir, 1) 
        assert ret.retval == files


    ret = gkfs_client.directory_validate(
                topdir, 1000) 
    assert ret.retval == 1000+3


def test_extended(gkfs_daemon, gkfs_shell, gkfs_client):
    topdir = gkfs_daemon.mountdir / "test_extended"
    dir_a  = topdir / "dir_a"
    dir_b  = topdir / "dir_b"
    file_a = topdir / "file_a"
    subdir_a  = dir_a / "subdir_a"

    # create topdir
    ret = gkfs_client.mkdir(
            topdir,
            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    ret = gkfs_client.mkdir(
            dir_a,
            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    ret = gkfs_client.mkdir(
            dir_b,
            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    ret = gkfs_client.mkdir(
            subdir_a,
            stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 0

    ret = gkfs_client.open(file_a,
                           os.O_CREAT,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    assert ret.retval != -1

    buf = b'42'
    ret = gkfs_client.write(file_a, buf, 1)

    assert ret.retval == 1

    cmd = gkfs_shell.sfind(
            topdir,
            '-M',
            gkfs_daemon.mountdir,
            '-S',
            1,
            '-name',
            '*_k*'
            )

    assert cmd.exit_code == 0
    assert cmd.stdout.decode() == "MATCHED 0/4\n"

@pytest.mark.skip(reason="invalid errno returned on success")
@pytest.mark.parametrize("directory_path",
    [ nonexisting ])
def test_opendir(gkfs_daemon, gkfs_client, directory_path):

    ret = gkfs_client.opendir(gkfs_daemon.mountdir / directory_path)

    assert ret.dirp is None
    assert ret.errno == errno.ENOENT

