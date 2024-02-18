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
import logging
from collections import namedtuple
from _pytest.logging import caplog as _caplog
from pathlib import Path
from harness.logger import logger, initialize_logging, finalize_logging
from harness.cli import add_cli_options, set_default_log_formatter
from harness.workspace import Workspace, FileCreator
from harness.gkfs import Daemon, Client, ShellClient, FwdDaemon, FwdClient, ShellFwdClient, FwdDaemonCreator, FwdClientCreator
from harness.reporter import report_test_status, report_test_headline, report_assertion_pass

def pytest_configure(config):
    """
    Some configurations for our particular usage of pytest
    """
    set_default_log_formatter(config, "%(message)s")

def pytest_assertion_pass(item, lineno, orig, expl):

    location = namedtuple(
            'Location', ['path', 'module', 'function', 'lineno'])(
                    str(item.parent.fspath), item.parent.name, item.name, lineno)

    report_assertion_pass(logger, location, orig, expl)

def pytest_addoption(parser):
    """
    Adds extra options from the GKFS harness to the py.test CLI.
    """
    add_cli_options(parser)

@pytest.fixture(autouse=True)
def caplog(test_workspace, request, _caplog):

    # we don't need to see the logs from sh
    _caplog.set_level(logging.CRITICAL, 'sh.command')
    _caplog.set_level(logging.CRITICAL, 'sh.command.process')
    _caplog.set_level(logging.CRITICAL, 'sh.command.process.streamreader')
    _caplog.set_level(logging.CRITICAL, 'sh.stream_bufferer')
    _caplog.set_level(logging.CRITICAL, 'sh.streamreader')

    test_log_path = test_workspace.logdir / (request.node.name + ".log")

    h = initialize_logging(logger, test_log_path)
    report_test_headline(logger, request.node.nodeid, request.config, test_workspace)

    yield _caplog

    finalize_logging(logger, h)

def pytest_runtest_logreport(report):
    """
    Pytest hook called after a test phase (setup, call, teardownd) 
    has completed.
    """

    report_test_status(logger, report)

@pytest.fixture
def test_workspace(tmp_path, request):
    """
    Initializes a test workspace by creating a temporary directory for it.
    """

    yield Workspace(tmp_path,
            request.config.getoption('--bin-dir'),
            request.config.getoption('--lib-dir'))

@pytest.fixture
def gkfs_daemon_rocksdb(test_workspace, request):
    """
    Initializes a local gekkofs daemon
    """

    interface = request.config.getoption('--interface')
    daemon = Daemon(interface, "rocksdb", test_workspace)

    yield daemon.run()
    daemon.shutdown()

@pytest.fixture
def gkfs_daemon_parallaxdb(test_workspace, request):
    """
    Initializes a local gekkofs daemon
    """

    interface = request.config.getoption('--interface')
    daemon = Daemon(interface, "parallaxdb", test_workspace)

    yield daemon.run()
    daemon.shutdown()

@pytest.fixture(params=['gkfs_daemon_rocksdb'])
def gkfs_daemon(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def gkfs_client(test_workspace):
    """
    Sets up a gekkofs client environment so that
    operations (system calls, library calls, ...) can
    be requested from a co-running daemon.
    """

    return Client(test_workspace)

@pytest.fixture
def gkfs_shell(test_workspace):
    """
    Sets up a gekkofs environment so that shell commands
    (stat, ls, mkdir, etc.) can be issued to a co-running daemon.
    """

    return ShellClient(test_workspace)

@pytest.fixture
def file_factory(test_workspace):
    """
    Returns a factory that can create custom input files
    in the test workspace.
    """

    return FileCreator(test_workspace)

@pytest.fixture
def gkfwd_daemon_factory(test_workspace, request):
    """
    Returns a factory that can create forwarding daemons
    in the test workspace.
    """

    interface = request.config.getoption('--interface')

    return FwdDaemonCreator(interface, test_workspace)

@pytest.fixture
def gkfwd_client_factory(test_workspace):
    """
    Sets up a gekkofs client environment so that
    operations (system calls, library calls, ...) can
    be requested from a co-running daemon.
    """

    return FwdClientCreator(test_workspace)
