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

#ifndef GKFS_COMMON_STATS_HPP
#define GKFS_COMMON_STATS_HPP

#include <cstdint>
#include <unistd.h>
#include <cassert>
#include <map>
#include <set>
#include <vector>
#include <deque>
#include <chrono>
#include <optional>
#include <initializer_list>
#include <thread>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <atomic>
#include <mutex>
#include <config.hpp>


// PROMETHEUS includes
#ifdef GKFS_ENABLE_PROMETHEUS
#include <prometheus/counter.h>
#include <prometheus/summary.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/gateway.h>

using namespace prometheus;
#endif


/**
 * Provides storage capabilities to provide stats about GekkoFS
 * The information is per server.
 * We do not provide accurate stats for 1-5-10 minute stats
 *
 */
namespace gkfs::utils {

/**
 *
 * Number of operations (Create, write/ read, remove, mkdir...)
 * Size of database (metadata keys, should be not needed, any)
 * Size of data (+write - delete)
 * Server Bandwidth (write / read operations)
 *
 * mean, (lifetime of the server)
 * 1 minute mean
 * 5 minute mean
 * 10 minute mean
 *
 * To provide the stats that we need,
 * we need to store the info and the timestamp to calculate it
 * A vector should work, with a maximum of elements,
 */

class Stats {
public:
    enum class IopsOp {
        iops_create,
        iops_write,
        iops_read,
        iops_stats,
        iops_dirent,
        iops_remove,
    }; ///< enum storing IOPS Stats

    enum class SizeOp { write_size, read_size }; ///< enum storing Size Stats

private:
    constexpr static const std::initializer_list<Stats::IopsOp> all_IopsOp = {
            IopsOp::iops_create, IopsOp::iops_write,
            IopsOp::iops_read,   IopsOp::iops_stats,
            IopsOp::iops_dirent, IopsOp::iops_remove}; ///< Enum IOPS iterator

    constexpr static const std::initializer_list<Stats::SizeOp> all_SizeOp = {
            SizeOp::write_size, SizeOp::read_size}; ///< Enum SIZE iterator

    const std::vector<std::string> IopsOp_s = {
            "IOPS_CREATE", "IOPS_WRITE",   "IOPS_READ",
            "IOPS_STATS",  "IOPS_DIRENTS", "IOPS_REMOVE"}; ///< Stats Labels
    const std::vector<std::string> SizeOp_s = {"WRITE_SIZE",
                                               "READ_SIZE"}; ///< Stats Labels

    std::chrono::time_point<std::chrono::steady_clock>
            start; ///< When we started the server


    std::map<IopsOp, std::atomic<unsigned long>>
            iops_mean; ///< Stores total value for global mean
    std::map<SizeOp, std::atomic<unsigned long>>
            size_mean; ///< Stores total value for global mean

    std::mutex time_iops_mutex;
    std::mutex size_iops_mutex;

    std::map<IopsOp,
             std::deque<std::chrono::time_point<std::chrono::steady_clock>>>
            time_iops; ///< Stores timestamp when an operation comes removes if
                       ///< first operation if > 10 minutes Different means will
                       ///< be stored and cached 1 minuted


    std::map<SizeOp, std::deque<std::pair<
                             std::chrono::time_point<std::chrono::steady_clock>,
                             unsigned long long>>>
            time_size; ///< For size operations we need to store the timestamp
                       ///< and the size


    std::thread t_output;    ///< Thread that outputs stats info
    bool output_thread_;     ///< Enables or disables the output thread
    bool enable_prometheus_; ///< Enables or disables the prometheus output
    bool enable_chunkstats_; ///< Enables or disables the chunk stats output


    bool running =
            true; ///< Controls the destruction of the class/stops the thread
    /**
     * @brief Sends all the stats to the screen
     * Debug Function
     *
     * @param d is the time between output
     * @param file_output is the output file
     */
    void
    output(std::chrono::seconds d, std::string file_output);

    std::map<std::pair<std::string, unsigned long long>,
             std::atomic<unsigned int>>
            chunk_reads; ///< Stores the number of times a chunk/file is read
    std::map<std::pair<std::string, unsigned long long>,
             std::atomic<unsigned int>>
            chunk_writes; ///< Stores the number of times a chunk/file is write

    /**
     * @brief Called by output to generate CHUNK map
     *
     * @param output is the output stream
     */
    void
    output_map(std::ofstream& output);


    /**
     * @brief Dumps all the means from the stats
     * @param of Output stream
     */
    void
    dump(std::ofstream& of);


// Prometheus Push structs
#ifdef GKFS_ENABLE_PROMETHEUS
    std::shared_ptr<Gateway> gateway;   ///< Prometheus Gateway
    std::shared_ptr<Registry> registry; ///< Prometheus Counters Registry
    Family<Counter>* family_counter;    ///< Prometheus IOPS counter (managed by
                                        ///< Prometheus cpp)
    Family<Summary>* family_summary;    ///< Prometheus SIZE counter (managed by
                                        ///< Prometheus cpp)
    std::map<IopsOp, Counter*> iops_prometheus; ///< Prometheus IOPS metrics
    std::map<SizeOp, Summary*> size_prometheus; ///< Prometheus SIZE metrics
#endif

public:
    /**
     * @brief Starts the Stats module and initializes structures
     * @param enable_chunkstats Enables or disables the chunk stats
     * @param enable_prometheus Enables or disables the prometheus output
     * @param filename file where to write the output
     * @param prometheus_gateway ip:port to expose the metrics
     */
    Stats(bool enable_chunkstats, bool enable_prometheus,
          const std::string& filename, const std::string& prometheus_gateway);

    /**
     * @brief Destroys the class, and any associated thread
     *
     */
    ~Stats();


    /**
     * @brief Set the up Prometheus gateway and structures
     *
     * @param gateway_ip ip of the prometheus gateway
     * @param gateway_port port of the prometheus gateway
     */
    void
    setup_Prometheus(const std::string& gateway_ip,
                     const std::string& gateway_port);

    /**
     * @brief Adds a new read access to the chunk/path specified
     *
     * @param path path of the chunk
     * @param chunk chunk number
     */
    void
    add_read(const std::string& path, unsigned long long chunk);
    /**
     * @brief Adds a new write access to the chunk/path specified
     *
     * @param path path of the chunk
     * @param chunk chunk number
     */
    void
    add_write(const std::string& path, unsigned long long chunk);


    /**
     * Add a new value for a IOPS, that does not involve any size
     * No value needed as they are simple (1 create, 1 read...)
     * Size operations internally call this operation (read,write)
     *
     * @param IopsOp Which operation to add
     */

    void add_value_iops(enum IopsOp);

    /**
     * @brief Store a new stat point, with a size value.
     * If it involves a IO operations it will call the corresponding
     * operation
     *
     * @param SizeOp Which operation we refer
     * @param value to store (SizeOp)
     */
    void
    add_value_size(enum SizeOp, unsigned long long value);

    /**
     * @brief Get the total mean value of the asked stat
     * This can be provided inmediately without cost
     * @param IopsOp Which operation to get
     * @return mean value
     */
    double get_mean(enum IopsOp);


    /**
     * @brief Get the total mean value of the asked stat
     * This can be provided inmediately without cost
     * @param SizeOp Which operation to get
     * @return mean value
     */
    double get_mean(enum SizeOp);

    /**
     * @brief Get all the means (total, 1,5 and 10 minutes) for a SIZE_OP
     * Returns precalculated values if we just calculated them 1 minute ago
     * @param SizeOp Which operation to get
     *
     * @return std::vector< double > with 4 means
     */
    std::vector<double> get_four_means(enum SizeOp);

    /**
     * @brief Get all the means (total, 1,5 and 10 minutes) for a IOPS_OP
     * Returns precalculated values if we just calculated them 1 minute ago
     * @param IopsOp Which operation to get
     *
     * @return std::vector< double > with 4 means
     */
    std::vector<double> get_four_means(enum IopsOp);
};

} // namespace gkfs::utils

#endif // GKFS_COMMON_STATS_HPP