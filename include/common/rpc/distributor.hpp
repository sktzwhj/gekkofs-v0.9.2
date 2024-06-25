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

#ifndef GEKKOFS_RPC_DISTRIBUTOR_HPP
#define GEKKOFS_RPC_DISTRIBUTOR_HPP

#include "../include/config.hpp"
#include <vector>
#include <string>
#include <numeric>
#include <unordered_map>
#include <fstream>
#include <map>

namespace gkfs::rpc {

using chunkid_t = unsigned int;
using host_t = unsigned int;

class Distributor {
public:
    virtual host_t
    localhost() const = 0;

    virtual host_t
    locate_fs(const std::string& path) const = 0;

    virtual host_t
    locate(const std::string& path, unsigned int hostnum, const int num_copy) const = 0;

    virtual host_t
    locate_data(const std::string& path, const chunkid_t& chnk_id,
                const int num_copy) const = 0;
    // TODO: We need to pass hosts_size in the server side, because the number
    // of servers are not defined (in startup)

    virtual unsigned int
    hosts_size() const = 0;

    virtual host_t
    locate_data(const std::string& path, const chunkid_t& chnk_id,
                unsigned int hosts_size, const int num_copy) = 0;

    virtual host_t
    locate_file_metadata(const std::string& path, const int num_copy) const = 0;

    virtual std::vector<host_t>
    locate_directory_metadata(const std::string& path) const = 0;
};


class SimpleHashDistributor : public Distributor {
private:
    host_t localhost_;
    host_t localfs_ = 0;
    std::vector<unsigned int> hosts_size_{0};
    std::vector<host_t> all_hosts_;
    std::hash<std::string> str_hash;
    std::map<std::string, unsigned int> * pathfs_{nullptr};

public:
    SimpleHashDistributor();

    SimpleHashDistributor(host_t localhost, std::vector<unsigned int> hosts_size, std::map<std::string, unsigned int> * pathfs, host_t localfs);

    unsigned int
    hosts_size() const override;
    
    host_t
    locate_fs(const std::string& path) const override;

    host_t
    locate(const std::string& path, unsigned int hostnum, const int num_copy) const override;
    
    host_t
    localhost() const override;

    host_t
    locate_data(const std::string& path, const chunkid_t& chnk_id,
                const int num_copy) const override;

    host_t
    locate_data(const std::string& path, const chunkid_t& chnk_id,
                unsigned int host_size, const int num_copy);

    host_t
    locate_file_metadata(const std::string& path,
                         const int num_copy) const override;

    std::vector<host_t>
    locate_directory_metadata(const std::string& path) const override;
};

class LocalOnlyDistributor : public Distributor {
private:
    host_t localhost_;
    unsigned int hosts_size_{0};

public:
    explicit LocalOnlyDistributor(host_t localhost);

    host_t
    localhost() const override;

    unsigned int
    hosts_size() const override;

    host_t
    locate_data(const std::string& path, const chunkid_t& chnk_id,
                const int num_copy) const override;

    host_t
    locate_file_metadata(const std::string& path,
                         const int num_copy) const override;

    std::vector<host_t>
    locate_directory_metadata(const std::string& path) const override;
};

class ForwarderDistributor : public Distributor {
private:
    host_t fwd_host_;
    unsigned int hosts_size_{0};
    std::vector<host_t> all_hosts_;
    std::hash<std::string> str_hash;

public:
    ForwarderDistributor(host_t fwhost, unsigned int hosts_size);

    host_t
    localhost() const override final;

    unsigned int
    hosts_size() const override;

    host_t
    locate_data(const std::string& path, const chunkid_t& chnk_id,
                const int num_copy) const override final;

    host_t
    locate_data(const std::string& path, const chunkid_t& chnk_id,
                unsigned int host_size, const int num_copy) override final;

    host_t
    locate_file_metadata(const std::string& path,
                         const int num_copy) const override;

    std::vector<host_t>
    locate_directory_metadata(const std::string& path) const override;
};

/*
 * Class IntervalSet
 * FROM
 *https://stackoverflow.com/questions/55646605/is-there-a-collection-for-storing-discrete-intervals
 **/
class IntervalSet {
    std::map<chunkid_t, chunkid_t> _intervals;

public:
    void Add(chunkid_t, chunkid_t);
    bool
    IsInsideInterval(unsigned int) const;
};

class GuidedDistributor : public Distributor {
private:
    host_t localhost_;
    unsigned int hosts_size_{0};
    std::vector<host_t> all_hosts_;
    std::hash<std::string> str_hash;
    std::unordered_map<std::string, std::pair<IntervalSet, unsigned int>>
            map_interval;
    std::vector<std::string> prefix_list; // Should not be very long
    bool
    init_guided();

public:
    GuidedDistributor();

    GuidedDistributor(host_t localhost, unsigned int hosts_size);

    host_t
    localhost() const override;

    unsigned int
    hosts_size() const override;

    host_t
    locate_data(const std::string& path, const chunkid_t& chnk_id,
                const int num_copy) const override;

    host_t
    locate_data(const std::string& path, const chunkid_t& chnk_id,
                unsigned int host_size, const int num_copy);

    host_t
    locate_file_metadata(const std::string& path,
                         const int num_copy) const override;

    std::vector<host_t>
    locate_directory_metadata(const std::string& path) const override;
};

} // namespace gkfs::rpc

#endif // GEKKOFS_RPC_LOCATOR_HPP
