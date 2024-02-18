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

from collections import namedtuple

class Md5sumOutputSchema:
    """
    Schema to deserialize the results of a md5sum command:

    $ md5sum foobar
    7f45c62700402ce5f9abe5b8d70d2844  foobar

    """

    _field_names = [ 'digest', 'filename' ]

    def loads(self, input):
        values = input.split()
        return namedtuple('md5sumOutput', self._field_names)(*values)

class StatOutputSchema:
    """
    Schema to deserialize the results of a stat --terse command:

        $ stat --terse foobar
        foobar 913 8 81b4 1000 1000 10308 7343758 1 0 0 1583160824 1583160634 1583160634 0 4096

        Output for the command follows the format below:

            %n %s %b %f %u %g %D %i %h %t %T %X %Y %Z %W %o %C

            %n: file name
            %s: total size, in bytes
            %b: number of blocks
            %f: raw mode in hex
            %u: owner UID
            %g: owner GID
            %D: device numer in hex
            %i: inode number
            %h: number of hard links
            %t: major device in hex
            %T: minor device in hex
            %X: time of last access, seconds since Epoch
            %Y: time of last data modification, seconds since Epoch
            %Z: time of last status change, seconds since Epoch
            %W: time of file birth, seconds since Epoch; 0 if unknown
            %o: optimal I/O transfer size hint
    """

    _field_names = [
        'filename', 'size', 'blocks', 'raw_mode', 'uid', 'gid', 'device',
        'inode', 'hard_links', 'major', 'minor', 'last_access',
        'last_modification', 'last_status_change', 'creation',
        'transfer_size' ]

    _field_types = [
        str, int, int, str, int, int, str, int, int, str, str, int, int, int,
        int, int, str ]

    def loads(self, input):
        values = [ t(s) for t,s in zip(self._field_types, input.split()) ]
        return namedtuple('statOutput', self._field_names)(*values)

class CommandParser:
    """
    A helper parser to transform the output of some shell commands to native
    Python objects.
    """

    OutputSchemas = {
        'md5sum'  : Md5sumOutputSchema(),
        'stat'    : StatOutputSchema(),
    }

    def parse(self, command, output):
        if command not in self.OutputSchemas:
            raise NotImplementedError(
                    f"Output parser for '{command}' not implemented")
        return self.OutputSchemas[command].loads(output)
