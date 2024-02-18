/*
  Copyright 2018-2024, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2024, Johannes Gutenberg Universitaet Mainz, Germany

  This software was partially supported by the
  EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).

  This software was partially supported by the
  ADA-FS project under the SPPEXA project funded by the DFG.

  This file is part of GekkoFS.

  GekkoFS is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  GekkoFS is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with GekkoFS.  If not, see <https://www.gnu.org/licenses/>.

  SPDX-License-Identifier: GPL-3.0-or-later
*/

#include <common/rpc/rpc_util.hpp>

extern "C" {
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
}

#include <system_error>

using namespace std;

namespace gkfs::rpc {

/**
 * converts std bool to mercury bool
 * @param state
 * @return
 */
hg_bool_t
bool_to_merc_bool(const bool state) {
    return state ? static_cast<hg_bool_t>(HG_TRUE)
                 : static_cast<hg_bool_t>(HG_FALSE);
}


/**
 * Returns the machine's hostname
 * @return
 */
string
get_my_hostname(bool short_hostname) {
    char hostname[1024];
    auto ret = gethostname(hostname, 1024);
    if(ret == 0) {
        string hostname_s(hostname);
        if(!short_hostname)
            return hostname_s;
        // get short hostname
        auto pos = hostname_s.find("."s);
        if(pos != string::npos)
            hostname_s = hostname_s.substr(0, pos);
        return hostname_s;
    } else
        return ""s;
}

#ifdef GKFS_ENABLE_UNUSED_FUNCTIONS
string
get_host_by_name(const string& hostname) {
    int err = 0;
    struct addrinfo hints;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_flags = (AI_V4MAPPED | AI_ADDRCONFIG);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_RAW;

    struct addrinfo* addr = nullptr;

    err = getaddrinfo(hostname.c_str(), nullptr, &hints, &addr);
    if(err) {
        throw runtime_error("Error getting address info for '" + hostname +
                            "': " + gai_strerror(err));
    }

    char addr_str[INET6_ADDRSTRLEN];

    err = getnameinfo(addr->ai_addr, addr->ai_addrlen, addr_str,
                      INET6_ADDRSTRLEN, nullptr, 0,
                      (NI_NUMERICHOST | NI_NOFQDN));
    if(err) {
        throw runtime_error("Error on getnameinfo(): "s + gai_strerror(err));
    }
    freeaddrinfo(addr);
    return addr_str;
}
#endif

} // namespace gkfs::rpc