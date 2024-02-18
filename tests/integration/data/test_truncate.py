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

# import harness
# from pathlib import Path
import os
import stat
import errno


# @pytest.mark.xfail(reason="invalid errno returned on success")
def test_truncate(gkfs_daemon, gkfs_client):
    """Testing truncate:
    1. create a large file over multiple chunks
    2. truncate it in the middle and compare it with a fresh file with equal contents (exactly at chunk border)
    3. truncate it again so that in truncates in the middle of the chunk and compare with fresh file
    TODO chunksize needs to be respected to make sure chunk border and in the middle of chunk truncates are honored
    """
    truncfile = gkfs_daemon.mountdir / "trunc_file"

    # open and create test file
    ret = gkfs_client.open(truncfile, os.O_CREAT | os.O_WRONLY, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval != -1

    # write a multi MB file (16mb)
    buf_length = 16777216
    ret = gkfs_client.write_random(truncfile, buf_length)

    assert ret.retval == buf_length

    ret = gkfs_client.stat(truncfile)

    assert ret.statbuf.st_size == buf_length

    # truncate it
    # split exactly in the middle
    trunc_size = buf_length // 2
    ret = gkfs_client.truncate(truncfile, trunc_size)

    assert ret.retval == 0
    # check file length
    ret = gkfs_client.stat(truncfile)

    assert ret.statbuf.st_size == trunc_size

    # verify contents by writing a new file (random content is seeded) and checksum both
    truncfile_verify = gkfs_daemon.mountdir / "trunc_file_verify"

    # open and create test file
    ret = gkfs_client.open(truncfile_verify, os.O_CREAT | os.O_WRONLY, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval != -1

    # write trunc_size of data to new file
    ret = gkfs_client.write_random(truncfile_verify, trunc_size)

    assert ret.retval == trunc_size

    ret = gkfs_client.stat(truncfile_verify)

    assert ret.statbuf.st_size == trunc_size

    ret = gkfs_client.file_compare(truncfile, truncfile_verify, trunc_size)

    assert ret.retval == 0

    # trunc at byte 712345 (middle of chunk)
    # TODO feed chunksize into test to make sure it is always in the middle of the chunk
    trunc_size = 712345
    ret = gkfs_client.truncate(truncfile, trunc_size)

    assert ret.retval == 0

    # check file length
    ret = gkfs_client.stat(truncfile)

    assert ret.statbuf.st_size == trunc_size

    # verify contents by writing a new file (random content is seeded) and checksum both
    truncfile_verify_2 = gkfs_daemon.mountdir / "trunc_file_verify_2"

    # open and create test file
    ret = gkfs_client.open(truncfile_verify_2, os.O_CREAT | os.O_WRONLY, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval != -1

    # write trunc_size of data to new file
    ret = gkfs_client.write_random(truncfile_verify_2, trunc_size)

    assert ret.retval == trunc_size

    ret = gkfs_client.stat(truncfile_verify_2)

    assert ret.statbuf.st_size == trunc_size

    ret = gkfs_client.file_compare(truncfile, truncfile_verify_2, trunc_size)

    assert ret.retval == 0


# Tests failure cases
def test_fail_truncate(gkfs_daemon, gkfs_client):
    truncfile = gkfs_daemon.mountdir / "trunc_file2"
    # open and create test file
    ret = gkfs_client.open(truncfile, os.O_CREAT | os.O_WRONLY, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval != -1

    buf_length = 256
    ret = gkfs_client.write_random(truncfile, buf_length)
    assert ret.retval == buf_length

    ret = gkfs_client.truncate(truncfile, buf_length)
    assert ret.retval == 0

    # Size should not change
    ret = gkfs_client.stat(truncfile)
    assert ret.statbuf.st_size == buf_length

    # Truncate to a negative size
    ret = gkfs_client.truncate(truncfile, -1)
    assert ret.retval == -1
    assert ret.errno == errno.EINVAL

    # Truncate to a size greater than the file size
    ret = gkfs_client.truncate(truncfile, buf_length + 1)
    assert ret.retval == 0

    
    # Size should increase
    ret = gkfs_client.stat(truncfile)
    assert ret.statbuf.st_size == buf_length+1


