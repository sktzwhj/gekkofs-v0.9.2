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

import _pytest
import logging
from pathlib import Path

### This code is meant to be included automatically by CMake in the build
### directory's top-level conftest.py as well as the source directory's
### conftest.py, so that tests can be correctly run from both directories
def add_cli_options(parser):
    """
    Adds extra options to the py.test CLI so that we can provide
    search directories for libraries and helper programs.
    """

    try:
        parser.addoption(
            '--interface',
            action='store',
            type=str,
            default='lo',
            help="network interface used for communications (default: 'lo')."
        )

        parser.addoption(
            "--bin-dir",
            action='append',
            default=[Path.cwd()],
            help="directory that should be considered when searching "
                "for programs (multi-allowed)."
        )

        parser.addoption(
            "--lib-dir",
            action='append',
            default=[Path.cwd()],
            help="directory that should be considered when searching "
                "for libraries (multi-allowed)."
        )
    except ValueError:
        # if the CLI args have already been added, we have been called both
        # from the build directory's conftest.py and from the source
        # directory's conftest.py through automatic finding, ignore the error
        pass


def set_default_log_formatter(config, fmt):

    plugin_class = config.pluginmanager.get_plugin('logging').LoggingPlugin

    if not isinstance(plugin_class, LoggingPlugin):
        config.pluginmanager.get_plugin('logging').LoggingPlugin = LoggingPlugin


class LoggingPlugin(_pytest.logging.LoggingPlugin):
    """
    Replacement logging plugin that rewrites py.test default log formatter
    """

    def _create_formatter(self, log_format,
                          log_date_format, auto_indent) -> logging.Formatter:
        """
        Patch pytest default logger to always return our formatter

        Returns:
            logging.Formatter: Our formatter
        """

        # since we use loguru for formatting, we just want the message
        return logging.Formatter("%(message)s")
