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


def test_rename(gkfs_daemon, gkfs_client):

    file = gkfs_daemon.mountdir / "file"
    file2 = gkfs_daemon.mountdir / "file2"

    # create a file in gekkofs
    ret = gkfs_client.open(file,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000
    
    ret = gkfs_client.access(file, os.R_OK)
    assert ret.retval == 0

    # write a buffer we know
    buf = b'42'
    ret = gkfs_client.write(file, buf, len(buf))

    assert ret.retval == len(buf) # Return the number of written bytes

    ret = gkfs_client.stat(file)
    assert ret.retval != 1

    ret = gkfs_client.stat(file2)
    assert ret.retval == -1

    ret = gkfs_client.rename(file, file2)
    assert ret.retval == 0

    ret = gkfs_client.access(file,  os.R_OK)
    assert ret.retval == -1

    ret = gkfs_client.access(file2, os.R_OK)
    assert ret.retval == 0

    ret = gkfs_client.stat(file)
    assert ret.retval == -1

    ret = gkfs_client.stat(file2)
    assert ret.retval != 1

    # File is renamed, and innacesible

    # Read contents of file2
    ret = gkfs_client.open(file2, os.O_RDONLY)
    assert ret.retval == 10000

    ret = gkfs_client.read(file2, len(buf))
    assert ret.retval == len(buf)
    assert ret.buf == buf

    

def test_rename_inverse(gkfs_daemon, gkfs_client):

    file3 = gkfs_daemon.mountdir / "file3"
    file4 = gkfs_daemon.mountdir / "file4"

    # create a file in gekkofs
    ret = gkfs_client.open(file3,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

 

    ret = gkfs_client.stat(file3)
    assert ret.retval != 1

    ret = gkfs_client.stat(file4)
    assert ret.retval == -1

    ret = gkfs_client.rename(file3, file4)

    assert ret.retval == 0

    ret = gkfs_client.stat(file3)
    assert ret.retval == -1

    ret = gkfs_client.stat(file4)
    assert ret.retval != 1

    # File is renamed, and innacesible

   # write a buffer we know
    buf = b'42'
    ret = gkfs_client.write(file4, buf, len(buf))

    assert ret.retval == len(buf) # Return the number of written bytes

    # Read contents of file2
    ret = gkfs_client.open(file4, os.O_RDONLY)
    assert ret.retval == 10000

    ret = gkfs_client.read(file4, len(buf))
    assert ret.retval == len(buf)
    assert ret.buf == buf

    # It should work but the data should be on file 2 really

def test_chain_rename(gkfs_daemon, gkfs_client):

    filea = gkfs_daemon.mountdir / "filea"
    fileb = gkfs_daemon.mountdir / "fileb"
    filec = gkfs_daemon.mountdir / "filec"
    filed = gkfs_daemon.mountdir / "filed"
    filee = gkfs_daemon.mountdir / "filee"

    # create a file in gekkofs
    ret = gkfs_client.open(filea,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    # write a buffer we know
    buf = b'42'
    ret = gkfs_client.write(filea, buf, len(buf))

    assert ret.retval == len(buf) # Return the number of written bytes

    ret = gkfs_client.stat(filea)
    assert ret.retval != 1

    ret = gkfs_client.stat(fileb)
    assert ret.retval == -1

    ret = gkfs_client.rename(filea, fileb)

    assert ret.retval == 0

    ret = gkfs_client.stat(filea)
    assert ret.retval == -1

    ret = gkfs_client.stat(fileb)
    assert ret.retval != 1

    # File is renamed, and innacesible

    # Read contents of file2
    ret = gkfs_client.open(fileb, os.O_RDONLY)
    assert ret.retval == 10000

    ret = gkfs_client.read(fileb, len(buf))
    assert ret.retval == len(buf)
    assert ret.buf == buf

    ret = gkfs_client.rename(filea, filec)

    #filea should be gone
    assert ret.retval == -1

    ret = gkfs_client.rename(fileb, filec)
    assert ret.retval == 0

    ret = gkfs_client.read(filec, len(buf))
    assert ret.retval == len(buf)
    assert ret.buf == buf

    ret = gkfs_client.rename(filec, filed)
    assert ret.retval == 0

    ret = gkfs_client.read(filed, len(buf))
    assert ret.retval == len(buf)
    assert ret.buf == buf


    ret = gkfs_client.rename(filed, filee)
    assert ret.retval == 0

    ret = gkfs_client.read(filee, len(buf))
    assert ret.retval == len(buf)
    assert ret.buf == buf


    # check the stat of old files
    ret = gkfs_client.stat(fileb)
    assert ret.retval == -1

    ret = gkfs_client.stat(filec)
    assert ret.retval == -1

    ret = gkfs_client.stat(filed)
    assert ret.retval == -1

    ret = gkfs_client.stat(filee)
    assert ret.retval == 0
  
def test_cyclic_rename(gkfs_daemon, gkfs_client):

    fileold = gkfs_daemon.mountdir / "fileold"
    filenew = gkfs_daemon.mountdir / "filenew"
    

    # create a file in gekkofs
    ret = gkfs_client.open(fileold,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    # write a buffer we know
    buf = b'42'
    ret = gkfs_client.write(fileold, buf, len(buf))

    assert ret.retval == len(buf) # Return the number of written bytes

    ret = gkfs_client.stat(fileold)
    assert ret.retval != 1

    ret = gkfs_client.stat(filenew)
    assert ret.retval == -1

    ret = gkfs_client.rename(fileold, filenew)

    assert ret.retval == 0

    ret = gkfs_client.stat(fileold)
    assert ret.retval == -1

    ret = gkfs_client.stat(filenew)
    assert ret.retval != 1

    
    #Cyclic rename
    ret = gkfs_client.rename(filenew, fileold)

    ret = gkfs_client.stat(fileold)
    assert ret.retval != -1

    ret = gkfs_client.stat(filenew)
    assert ret.retval == -1
    # Read contents of filee
    ret = gkfs_client.open(fileold, os.O_RDONLY)
    assert ret.retval == 10000

    ret = gkfs_client.read(fileold, len(buf))
    assert ret.retval == len(buf)
    assert ret.buf == buf

def test_rename_plus_trunc(gkfs_daemon, gkfs_client):

    fileold = gkfs_daemon.mountdir / "fileoldtr"
    filenew = gkfs_daemon.mountdir / "filenewtr"
    

    # create a file in gekkofs
    ret = gkfs_client.open(fileold,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    # write a buffer we know
    buf = b'42'
    ret = gkfs_client.write(fileold, buf, len(buf))

    assert ret.retval == len(buf) # Return the number of written bytes

    # rename file
    ret = gkfs_client.rename(fileold, filenew)
    assert ret.retval == 0

    ret = gkfs_client.stat(filenew)
    assert ret.retval != -1
    assert ret.statbuf.st_size == len(buf)

    # truncate file
    ret = gkfs_client.truncate(filenew, 1)
    assert ret.retval == 0
    
    ret = gkfs_client.read(filenew, len(buf))
    assert ret.retval == 1
    assert ret.buf == b'4\x00'  # buf includes the null byte

    ret = gkfs_client.stat(filenew)
    assert ret.retval != -1
    assert ret.statbuf.st_size == 1

def test_rename_plus_lseek(gkfs_daemon, gkfs_client):

    fileold = gkfs_daemon.mountdir / "fileoldlseek"
    filenew = gkfs_daemon.mountdir / "filenewlseek"
    

    # create a file in gekkofs
    ret = gkfs_client.open(fileold,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    # write a buffer we know
    buf = b'42'
    ret = gkfs_client.write(fileold, buf, len(buf))

    assert ret.retval == len(buf) # Return the number of written bytes

    # rename file
    ret = gkfs_client.rename(fileold, filenew)
    assert ret.retval == 0

    ret = gkfs_client.stat(filenew)
    assert ret.retval != -1
    assert ret.statbuf.st_size == len(buf)

    ret = gkfs_client.lseek(filenew, 0, os.SEEK_END)
    assert ret.retval == 2                      #Two bytes written
    


def test_rename_delete(gkfs_daemon, gkfs_client):

    fileold = gkfs_daemon.mountdir / "fileoldrename"
    filenew = gkfs_daemon.mountdir / "filenewrename"
    

    # create a file in gekkofs
    ret = gkfs_client.open(fileold,
                           os.O_CREAT | os.O_WRONLY,
                           stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    assert ret.retval == 10000

    # write a buffer we know
    buf = b'42'
    ret = gkfs_client.write(fileold, buf, len(buf))
    assert ret.retval == len(buf) # Return the number of written bytes

    # rename file
    ret = gkfs_client.rename(fileold, filenew)
    assert ret.retval == 0

    ret = gkfs_client.stat(filenew)
    assert ret.retval != -1
    assert ret.statbuf.st_size == len(buf)

    ret = gkfs_client.readdir(gkfs_daemon.mountdir)
    assert len(ret.dirents) == 1

    ret = gkfs_client.unlink(fileold) # Remove original file (error)
    assert ret.retval != 0

    ret = gkfs_client.unlink(filenew) # Remove renamed file (success)
    assert ret.retval == 0        

    ret = gkfs_client.stat(filenew)
    assert ret.retval == -1

    ret = gkfs_client.readdir(gkfs_daemon.mountdir)
    assert len(ret.dirents) == 0
    


