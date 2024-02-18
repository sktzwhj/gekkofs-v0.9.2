#!/bin/bash
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

# optimal $GKFS_FIND_PROCESS is $GKFS_SERVERS+1 as we align servers and find processes if the $NUM_NODES are the same
# Output is saved to a file, so it can be processed by io500
srun --nvram-options=1LM:1980 -N $NUM_NODES -n $GKFS_FIND_PROCESS --export=ALL,PSM2_DEVICES=self,hfi,shm,PSM2_MULTIRAIL=1,PSM2_MULTI_EP=0,LD_PRELOAD=${GKFS_PRLD} -o find_${SLURM_JOB_ID}.txt $GKFS_FIND $@ -M $GKFS_MNT -S $GKFS_SERVERS
tail -n1 find_${SLURM_JOB_ID}.txt
exit 0


