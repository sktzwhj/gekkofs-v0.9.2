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


#include <common/statistics/stats.hpp>

using namespace std;

namespace gkfs::utils {

#ifdef GKFS_ENABLE_PROMETHEUS
static std::string
GetHostName() {
    char hostname[1024];

    if(::gethostname(hostname, sizeof(hostname))) {
        return {};
    }
    return hostname;
}
#endif

void
Stats::setup_Prometheus(const std::string& gateway_ip,
                        const std::string& gateway_port) {
// Prometheus Push model. Gateway
#ifdef GKFS_ENABLE_PROMETHEUS
    const auto labels = Gateway::GetInstanceLabel(GetHostName());
    gateway = std::make_shared<Gateway>(gateway_ip, gateway_port, "GekkoFS",
                                        labels);

    registry = std::make_shared<Registry>();
    family_counter = &BuildCounter()
                              .Name("IOPS")
                              .Help("Number of IOPS")
                              .Register(*registry);

    for(auto e : all_IopsOp) {
        iops_prometheus[e] = &family_counter->Add(
                {{"operation", IopsOp_s[static_cast<int>(e)]}});
    }

    family_summary = &BuildSummary()
                              .Name("SIZE")
                              .Help("Size of OPs")
                              .Register(*registry);

    for(auto e : all_SizeOp) {
        size_prometheus[e] = &family_summary->Add(
                {{"operation", SizeOp_s[static_cast<int>(e)]}},
                Summary::Quantiles{});
    }

    gateway->RegisterCollectable(registry);
#endif /// GKFS_ENABLE_PROMETHEUS
}

Stats::Stats(bool enable_chunkstats, bool enable_prometheus,
             const std::string& stats_file,
             const std::string& prometheus_gateway)
    : enable_prometheus_(enable_prometheus),
      enable_chunkstats_(enable_chunkstats) {

    // Init clocks
    start = std::chrono::steady_clock::now();

    // To simplify the control we add an element into the different maps
    // Statistaclly will be negligible... and we get a faster flow

    for(auto e : all_IopsOp) {
        iops_mean[e] = 0;
        time_iops[e].push_back(std::chrono::steady_clock::now());
    }

    for(auto e : all_SizeOp) {
        size_mean[e] = 0;
        time_size[e].push_back(pair(std::chrono::steady_clock::now(), 0.0));
    }

#ifdef GKFS_ENABLE_PROMETHEUS
    auto pos_separator = prometheus_gateway.find(':');
    setup_Prometheus(prometheus_gateway.substr(0, pos_separator),
                     prometheus_gateway.substr(pos_separator + 1));
#endif

    if(!stats_file.empty() || enable_prometheus_) {
        output_thread_ = true;
        t_output = std::thread([this, stats_file] {
            output(std::chrono::duration(10s), stats_file);
        });
    }
}

Stats::~Stats() {
    if(output_thread_) {
        running = false;
        if(t_output.joinable())
            t_output.join();
    }
}

void
Stats::add_read(const std::string& path, unsigned long long chunk) {
    chunk_reads[pair(path, chunk)]++;
}

void
Stats::add_write(const std::string& path, unsigned long long chunk) {
    chunk_writes[pair(path, chunk)]++;
}


void
Stats::output_map(std::ofstream& output) {
    // Ordering
    map<unsigned int, std::set<pair<std::string, unsigned long long>>>
            order_write;

    map<unsigned int, std::set<pair<std::string, unsigned long long>>>
            order_read;

    for(const auto& i : chunk_reads) {
        order_read[i.second].insert(i.first);
    }

    for(const auto& i : chunk_writes) {
        order_write[i.second].insert(i.first);
    }

    auto chunkMap =
            [](std::string caption,
               map<unsigned int,
                   std::set<pair<std::string, unsigned long long>>>& order,
               std::ofstream& output) {
                output << caption << std::endl;
                for(auto k : order) {
                    output << k.first << " -- ";
                    for(auto v : k.second) {
                        output << v.first << " // " << v.second << endl;
                    }
                }
            };

    chunkMap("READ CHUNK MAP", order_read, output);
    chunkMap("WRITE CHUNK MAP", order_write, output);
}

void
Stats::add_value_iops(enum IopsOp iop) {
    iops_mean[iop]++;
    auto now = std::chrono::steady_clock::now();

    const std::lock_guard<std::mutex> lock(time_iops_mutex);
    if((now - time_iops[iop].front()) > std::chrono::duration(10s)) {
        time_iops[iop].pop_front();
    } else if(time_iops[iop].size() >= gkfs::config::stats::max_stats)
        time_iops[iop].pop_front();

    time_iops[iop].push_back(std::chrono::steady_clock::now());
#ifdef GKFS_ENABLE_PROMETHEUS
    if(enable_prometheus_) {
        iops_prometheus[iop]->Increment();
    }
#endif
}

void
Stats::add_value_size(enum SizeOp iop, unsigned long long value) {
    auto now = std::chrono::steady_clock::now();
    size_mean[iop] += value;
    const std::lock_guard<std::mutex> lock(size_iops_mutex);
    if((now - time_size[iop].front().first) > std::chrono::duration(10s)) {
        time_size[iop].pop_front();
    } else if(time_size[iop].size() >= gkfs::config::stats::max_stats)
        time_size[iop].pop_front();

    time_size[iop].push_back(pair(std::chrono::steady_clock::now(), value));
#ifdef GKFS_ENABLE_PROMETHEUS
    if(enable_prometheus_) {
        size_prometheus[iop]->Observe(value);
    }
#endif
    if(iop == SizeOp::read_size)
        add_value_iops(IopsOp::iops_read);
    else if(iop == SizeOp::write_size)
        add_value_iops(IopsOp::iops_write);
}

/**
 * @brief Get the total mean value of the asked stat
 * This can be provided inmediately without cost
 * @return mean value
 */
double
Stats::get_mean(enum SizeOp sop) {
    auto now = std::chrono::steady_clock::now();
    auto duration =
            std::chrono::duration_cast<std::chrono::seconds>(now - start);
    double value = static_cast<double>(size_mean[sop]) /
                   static_cast<double>(duration.count());
    return value;
}

double
Stats::get_mean(enum IopsOp iop) {
    auto now = std::chrono::steady_clock::now();
    auto duration =
            std::chrono::duration_cast<std::chrono::seconds>(now - start);
    double value = static_cast<double>(iops_mean[iop]) /
                   static_cast<double>(duration.count());
    return value;
}


std::vector<double>
Stats::get_four_means(enum SizeOp sop) {
    std::vector<double> results = {0, 0, 0, 0};
    auto now = std::chrono::steady_clock::now();
    const std::lock_guard<std::mutex> lock(size_iops_mutex);
    for(auto e : time_size[sop]) {
        auto duration =
                std::chrono::duration_cast<std::chrono::minutes>(now - e.first)
                        .count();
        if(duration > 10)
            break;

        results[3] += e.second;
        if(duration > 5)
            continue;
        results[2] += e.second;
        if(duration > 1)
            continue;
        results[1] += e.second;
    }
    // Mean in MB/s
    results[0] = get_mean(sop) / (1024.0 * 1024.0);
    results[3] /= 10 * 60 * (1024.0 * 1024.0);
    results[2] /= 5 * 60 * (1024.0 * 1024.0);
    results[1] /= 60 * (1024.0 * 1024.0);

    return results;
}


std::vector<double>
Stats::get_four_means(enum IopsOp iop) {
    std::vector<double> results = {0, 0, 0, 0};
    auto now = std::chrono::steady_clock::now();
    const std::lock_guard<std::mutex> lock(time_iops_mutex);
    for(auto e : time_iops[iop]) {
        auto duration =
                std::chrono::duration_cast<std::chrono::minutes>(now - e)
                        .count();
        if(duration > 10)
            break;

        results[3]++;
        if(duration > 5)
            continue;
        results[2]++;
        if(duration > 1)
            continue;
        results[1]++;
    }

    results[0] = get_mean(iop);
    results[3] /= 10 * 60;
    results[2] /= 5 * 60;
    results[1] /= 60;

    return results;
}

void
Stats::dump(std::ofstream& of) {
    for(auto e : all_IopsOp) {
        auto tmp = get_four_means(e);

        of << "Stats " << IopsOp_s[static_cast<int>(e)]
           << " IOPS/s (avg, 1 min, 5 min, 10 min) \t\t";
        for(auto mean : tmp) {
            of << std::setprecision(4) << std::setw(9) << mean << " - ";
        }
        of << std::endl;
    }
    for(auto e : all_SizeOp) {
        auto tmp = get_four_means(e);

        of << "Stats " << SizeOp_s[static_cast<int>(e)]
           << " MB/s (avg, 1 min, 5 min, 10 min) \t\t";
        for(auto mean : tmp) {
            of << std::setprecision(4) << std::setw(9) << mean << " - ";
        }
        of << std::endl;
    }
    of << std::endl;
}
void
Stats::output(std::chrono::seconds d, std::string file_output) {
    int times = 0;
    std::optional<std::ofstream> of;
    if(!file_output.empty())
        of = std::ofstream(file_output, std::ios_base::openmode::_S_trunc);

    while(running) {
        if(of)
            dump(of.value());
        std::chrono::seconds a = 0s;

        times++;

        if(enable_chunkstats_ && of) {
            if(times % 4 == 0)
                output_map(of.value());
        }
#ifdef GKFS_ENABLE_PROMETHEUS
        if(enable_prometheus_) {
            gateway->Push();
        }
#endif
        while(running && a < d) {
            a += 1s;
            std::this_thread::sleep_for(1s);
        }
    }
}

} // namespace gkfs::utils
