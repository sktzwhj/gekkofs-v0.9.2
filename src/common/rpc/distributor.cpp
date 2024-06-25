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

#include <common/rpc/distributor.hpp>

using namespace std;

namespace gkfs {

namespace rpc {
/**
 * --Multiple GekkoFS--
 * Client distributor construction
 * @param localhost local daemon id
 * @param hosts_size vector: hostsize of each GekkoFS
 * @param pathfs cache: save GekkoFS id where path exists
 * @param localfs id of GekkoFS having local daemon
 */
SimpleHashDistributor::SimpleHashDistributor(host_t localhost,
                                             std::vector<unsigned int> hosts_size,
                                             std::map<std::string, unsigned int>* pathfs,
                                             host_t localfs)
    : localhost_(localhost), hosts_size_(hosts_size), pathfs_(pathfs), localfs_(localfs),
    all_hosts_(
        std::accumulate(hosts_size.begin(), hosts_size.end(), 0, [](unsigned int x, unsigned int y) {
        return x+y;})
    ) {
    ::iota(all_hosts_.begin(), all_hosts_.end(), 0);
}

SimpleHashDistributor::SimpleHashDistributor() {}

/* unused */
unsigned int
SimpleHashDistributor::hosts_size() const {
    return 0;
}

host_t
SimpleHashDistributor::localhost() const {
    return localhost_;
}

/**
 * --Multiple GekkoFS--
 * Locate GekkoFS where path exists
 * Two possible fs: local GekkoFS id, FS id in cache(saved by forward_stat)
 * @param path
 */
host_t
SimpleHashDistributor::locate_fs(const std::string& path) const{
    unsigned int fs = localfs_;
    if(pathfs_ && pathfs_->count(path)) fs= (*pathfs_)[path];
    return fs;
}

/**
 * --Multiple GekkoFS--
 * Only used for forward_getSuccessThread
 */
host_t
SimpleHashDistributor::locate(const std::string& path, unsigned int hostnum, const int num_copy) const{
    return (str_hash(path) + num_copy) % hostnum;
}

/**
 * --Multiple GekkoFS--
 * Locate host(daemon) id for data
 * Only used at Client: first locate fs id, second locate host(daemon) id
 */
host_t
SimpleHashDistributor::locate_data(const string& path, const chunkid_t& chnk_id,
                                   const int num_copy) const {
    unsigned int fs_id = localfs_;
    if(pathfs_ && pathfs_->count(path)) fs_id = (*pathfs_)[path];
    unsigned int prefix_hosts = 0;
    for(unsigned int fs = 0 ;fs < fs_id ; fs++)
        prefix_hosts += hosts_size_[fs];
    return (str_hash(path + ::to_string(chnk_id)) + num_copy) % hosts_size_.at(fs_id) + prefix_hosts;
}

/**
 * --Multiple GekkoFS--
 * Locate host(daemon) id for data
 * Only used at Daemon: pathfs_ is nullptr, localfs_ is default 0, host_size_ length is 1
 */
host_t
SimpleHashDistributor::locate_data(const string& path, const chunkid_t& chnk_id,
                                   unsigned int hosts_size,
                                   const int num_copy) {
    if(hosts_size_.at(0) != hosts_size) {
        hosts_size_.at(0) = hosts_size;
        all_hosts_ = std::vector<unsigned int>(hosts_size);
        ::iota(all_hosts_.begin(), all_hosts_.end(), 0);
    }
    unsigned int fs_id = localfs_;
    if(pathfs_ && pathfs_->count(path)) fs_id = (*pathfs_)[path];
    unsigned int prefix_hosts = 0;
    for(unsigned int fs = 0 ;fs < fs_id ; fs++)
        prefix_hosts += hosts_size_[fs];
    return (str_hash(path + ::to_string(chnk_id)) + num_copy) % hosts_size_.at(fs_id) + prefix_hosts;
}

/**
 * --Multiple GekkoFS--
 * Locate host(daemon) id for metadata
 * Only used at Client: first locate fs id, second locate host(daemon) id
 */
host_t
SimpleHashDistributor::locate_file_metadata(const string& path,
                                            const int num_copy) const {
    unsigned int fs_id = localfs_;
    if(pathfs_ && pathfs_->count(path)) fs_id = (*pathfs_)[path];
    unsigned int prefix_hosts = 0;
    for(unsigned int fs = 0 ;fs < fs_id ; fs++)
        prefix_hosts += hosts_size_[fs];
    return (str_hash(path) + num_copy) % hosts_size_.at(fs_id) + prefix_hosts;
}

/**
 * --Multiple GekkoFS--
 * Locate host(daemon) ids for dir metadata
 * Only used at Client
 */
::vector<host_t>
SimpleHashDistributor::locate_directory_metadata(const string& path) const {
    if(path == "/")
        return all_hosts_;
    if(pathfs_ && pathfs_->count(path)){
        unsigned int fs_id = (*pathfs_)[path];
        unsigned int prefix_hosts = 0;
        for(unsigned int fs = 0 ;fs < fs_id ; fs++)
            prefix_hosts += hosts_size_[fs];
        vector<host_t> target_hosts(all_hosts_.begin() + prefix_hosts, 
                                    all_hosts_.begin() + prefix_hosts + hosts_size_[fs_id]);
        return target_hosts;   
    } 
    return all_hosts_;
}

LocalOnlyDistributor::LocalOnlyDistributor(host_t localhost)
    : localhost_(localhost) {}

host_t
LocalOnlyDistributor::localhost() const {
    return localhost_;
}

unsigned int
LocalOnlyDistributor::hosts_size() const {
    return hosts_size_;
}

host_t
LocalOnlyDistributor::locate_data(const string& path, const chunkid_t& chnk_id,
                                  const int num_copy) const {
    return localhost_;
}

host_t
LocalOnlyDistributor::locate_file_metadata(const string& path,
                                           const int num_copy) const {
    return localhost_;
}

::vector<host_t>
LocalOnlyDistributor::locate_directory_metadata(const string& path) const {
    return {localhost_};
}

ForwarderDistributor::ForwarderDistributor(host_t fwhost,
                                           unsigned int hosts_size)
    : fwd_host_(fwhost), hosts_size_(hosts_size), all_hosts_(hosts_size) {
    ::iota(all_hosts_.begin(), all_hosts_.end(), 0);
}

host_t
ForwarderDistributor::localhost() const {
    return fwd_host_;
}

unsigned int
ForwarderDistributor::hosts_size() const {
    return hosts_size_;
}

host_t
ForwarderDistributor::locate_data(const std::string& path,
                                  const chunkid_t& chnk_id,
                                  const int num_copy) const {
    return fwd_host_;
}

host_t
ForwarderDistributor::locate_data(const std::string& path,
                                  const chunkid_t& chnk_id,
                                  unsigned int host_size, const int num_copy) {
    return fwd_host_;
}

host_t
ForwarderDistributor::locate_file_metadata(const std::string& path,
                                           const int num_copy) const {
    return (str_hash(path) + num_copy) % hosts_size_;
}


std::vector<host_t>
ForwarderDistributor::locate_directory_metadata(const std::string& path) const {
    return all_hosts_;
}

void
IntervalSet::Add(chunkid_t smaller, chunkid_t bigger) {
    const auto next = _intervals.upper_bound(smaller);
    if(next != _intervals.cbegin()) {
        const auto prev = std::prev(next);
        if(next != _intervals.cend() && next->first <= bigger + 1) {
            bigger = next->second;
            _intervals.erase(next);
        }
        if(prev->second + 1 >= smaller) {
            smaller = prev->first;
            _intervals.erase(prev);
        }
    }
    _intervals[smaller] = bigger;
}

bool
IntervalSet::IsInsideInterval(unsigned int v) const {
    const auto suspectNext = _intervals.upper_bound(v);
    const auto suspect = std::prev(suspectNext);
    return suspect->first <= v && v <= suspect->second;
}

bool
GuidedDistributor::init_guided() {
    unsigned int destination_host;
    chunkid_t chunk_id;
    string path;
    std::ifstream mapfile;
    mapfile.open(GKFS_USE_GUIDED_DISTRIBUTION_PATH);
    if((mapfile.rdstate() & std::ifstream::failbit) != 0)
        return false; // If it fails, the mapping will be as the SimpleHash

    while(mapfile >> path >> chunk_id >> destination_host) {
        // We need destination+1, as 0 has an special meaning in the interval
        // map.
        if(path.size() > 0 and path[0] == '#') {
            // Path that has this prefix will have metadata and data in the same
            // place  i.e. #/mdtest-hard/ 10 10 chunk_id and destination_host
            // are not used
            prefix_list.emplace_back(path.substr(1, path.size()));
            continue;
        }

        auto I = map_interval.find(path);
        if(I == map_interval.end()) {
            auto tmp = IntervalSet();
            tmp.Add(chunk_id, chunk_id + 1);
            map_interval[path] = make_pair(tmp, destination_host + 1);
        } else if(I->second.first.IsInsideInterval(chunk_id)) {
            auto is = I->second.first;
            is.Add(chunk_id, chunk_id + 1);
            I->second = (make_pair(is, destination_host + 1));
        }
    }
    mapfile.close();
    return true;
}

GuidedDistributor::GuidedDistributor() {
    init_guided();
}

GuidedDistributor::GuidedDistributor(host_t localhost,
                                     unsigned int hosts_size) {
    if(hosts_size_ != hosts_size) {
        hosts_size_ = hosts_size;
        localhost_ = localhost;
        all_hosts_ = std::vector<unsigned int>(hosts_size);
        ::iota(all_hosts_.begin(), all_hosts_.end(), 0);
    }
    init_guided();
}

host_t
GuidedDistributor::localhost() const {
    return localhost_;
}

unsigned int
GuidedDistributor::hosts_size() const {
    return hosts_size_;
}

host_t
GuidedDistributor::locate_data(const string& path, const chunkid_t& chnk_id,
                               unsigned int hosts_size, const int num_copy) {
    if(hosts_size_ != hosts_size) {
        hosts_size_ = hosts_size;
        all_hosts_ = std::vector<unsigned int>(hosts_size);
        ::iota(all_hosts_.begin(), all_hosts_.end(), 0);
    }

    return (locate_data(path, chnk_id, num_copy));
}

host_t
GuidedDistributor::locate_data(const string& path, const chunkid_t& chnk_id,
                               const int num_copy) const {
    auto it = map_interval.find(path);
    if(it != map_interval.end()) {
        auto it_f = it->second.first.IsInsideInterval(chnk_id);
        if(it_f) {
            return (it->second.second -
                    1); // Decrement destination host from the interval_map
        }
    }

    for(auto const& it : prefix_list) {
        if(0 == path.compare(0, min(it.length(), path.length()), it, 0,
                             min(it.length(), path.length()))) {
        }
        return str_hash(path) % hosts_size_;
    }

    auto locate = path + ::to_string(chnk_id);
    return (str_hash(locate) + num_copy) % hosts_size_;
}

host_t
GuidedDistributor::locate_file_metadata(const string& path,
                                        const int num_copy) const {
    return (str_hash(path) + num_copy) % hosts_size_;
}


::vector<host_t>
GuidedDistributor::locate_directory_metadata(const string& path) const {
    return all_hosts_;
}

} // namespace rpc
} // namespace gkfs
