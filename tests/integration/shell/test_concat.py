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

import pytest
from harness.logger import logger

large_file_01 = 'large_file_01'
large_file_02 = 'large_file_02'

@pytest.mark.skip(reason="using >> to concatenate files seems to hang clients")
def test_concat(gkfs_daemon, gkfs_shell, file_factory):
    """Concatenate two large files"""

    lf01 = file_factory.create(large_file_01, size=4.0, unit='MB')
    lf02 = file_factory.create(large_file_02, size=4.0, unit='MB')

    cmd = gkfs_shell.cp(lf01.pathname, gkfs_daemon.mountdir)
    assert cmd.exit_code == 0

    cmd = gkfs_shell.cp(lf02.pathname, gkfs_daemon.mountdir)
    assert cmd.exit_code == 0

    cmd = gkfs_shell.stat('--terse', gkfs_daemon.mountdir / large_file_01)
    assert cmd.exit_code == 0
    out = cmd.parsed_stdout
    assert out.size == lf01.size

    cmd = gkfs_shell.stat('--terse', gkfs_daemon.mountdir / large_file_02)
    assert cmd.exit_code == 0
    out = cmd.parsed_stdout
    assert out.size == lf02.size

    cmd = gkfs_shell.md5sum(gkfs_daemon.mountdir / large_file_01)
    assert cmd.exit_code == 0
    assert cmd.parsed_stdout.digest == lf01.md5sum()

    cmd = gkfs_shell.md5sum(gkfs_daemon.mountdir / large_file_02)
    assert cmd.exit_code == 0
    assert cmd.parsed_stdout.digest == lf02.md5sum()

    ##XXX hangs!
    cmd = gkfs_client.bash('cat', gkfs_daemon.mountdir / large_file_01, '>>', gkfs_daemon.mountdir / large_file_02)
    assert cmd.exit_code == 0
