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

FROM ruby:slim


RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        libssl-dev \
        libicu-dev \
        make \
        cmake \
        g++ \
        pkg-config \
    && \
    git clone https://github.com/cloudposse/copyright-header.git /tmp/copyright-header

COPY patch.diff /tmp/copyright-header

WORKDIR /tmp/copyright-header

RUN git apply patch.diff && \
    gem build copyright-header.gemspec && \
    gem install copyright-header-*.gem && \
    # cleanup
    rm -rf /tmp/copyright-header && \
    apt-get purge -y \
        git \
        libssl-dev \
        libicu-dev \
        make \
        cmake \
        g++ \
        pkg-config \
    && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean && \
    apt-get autoclean

VOLUME ["/usr/src"]

WORKDIR /usr/src

ENTRYPOINT ["/usr/local/bundle/bin/copyright-header"]
