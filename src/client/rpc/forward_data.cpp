/*
  Copyright 2018-2024, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2024, Johannes Gutenberg Universitaet Mainz, Germany

  This software was partially supported by the
  EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).

  This software was partially supported by the
  ADA-FS project under the SPPEXA project funded by the DFG.

  This file is part of GekkoFS' POSIX interface.

  GekkoFS' POSIX interface is free software: you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public License as
  published by the Free Software Foundation, either version 3 of the License,
  or (at your option) any later version.

  GekkoFS' POSIX interface is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with GekkoFS' POSIX interface.  If not, see
  <https://www.gnu.org/licenses/>.

  SPDX-License-Identifier: LGPL-3.0-or-later
*/

#include <client/preload_util.hpp>
#include <client/rpc/forward_data.hpp>
#include <client/rpc/rpc_types.hpp>
#include <client/logging.hpp>

#include <common/rpc/distributor.hpp>
#include <common/arithmetic/arithmetic.hpp>
#include <common/rpc/rpc_util.hpp>

#include <unordered_set>

using namespace std;

namespace gkfs::rpc {

/*
 * This file includes all metadata RPC calls.
 * NOTE: No errno is defined here!
 */

/**
 * Send an RPC request to write from a buffer.
 * There is a bitset of 1024 chunks to tell the server
 * which chunks to process. Exceeding this value will work without
 * replication. Another way is to leverage mercury segments.
 * TODO: Decide how to manage a write to a replica that doesn't exist
 * @param path
 * @param buf
 * @param append_flag
 * @param write_size
 * @param num_copies number of replicas
 * @return pair<error code, written size>
 */
pair<int, ssize_t>
forward_write(const string& path, const void* buf, const off64_t offset,
              const size_t write_size, const int8_t num_copies) {

    // import pow2-optimized arithmetic functions
    using namespace gkfs::utils::arithmetic;

    assert(write_size > 0);

    // Calculate chunkid boundaries and numbers so that daemons know in
    // which interval to look for chunks
    auto chnk_start = block_index(offset, gkfs::config::rpc::chunksize);
    auto chnk_end = block_index((offset + write_size) - 1,
                                gkfs::config::rpc::chunksize);

    auto chnk_total = (chnk_end - chnk_start) + 1;

    // Collect all chunk ids within count that have the same destination so
    // that those are send in one rpc bulk transfer
    std::map<uint64_t, std::vector<uint64_t>> target_chnks{};

    // contains the target ids, used to access the target_chnks map.
    // First idx is chunk with potential offset
    std::vector<uint64_t> targets{};

    // targets for the first and last chunk as they need special treatment
    // We need a set to manage replicas.
    std::set<uint64_t> chnk_start_target{};
    std::set<uint64_t> chnk_end_target{};

    std::unordered_map<uint64_t, std::vector<uint8_t>> write_ops_vect;

    // If num_copies is 0, we do the normal write operation. Otherwise
    // we process all the replicas.
    for(uint64_t chnk_id = chnk_start; chnk_id <= chnk_end; chnk_id++) {
        for(auto copy = num_copies ? 1 : 0; copy < num_copies + 1; copy++) {
            auto target = CTX->distributor()->locate_data(path, chnk_id, copy);

            if(write_ops_vect.find(target) == write_ops_vect.end())
                write_ops_vect[target] =
                        std::vector<uint8_t>(((chnk_total + 7) / 8));
            gkfs::rpc::set_bitset(write_ops_vect[target], chnk_id - chnk_start);

            if(target_chnks.count(target) == 0) {
                target_chnks.insert(
                        std::make_pair(target, std::vector<uint64_t>{chnk_id}));
                targets.push_back(target);
            } else {
                target_chnks[target].push_back(chnk_id);
            }

            // set first and last chnk targets
            if(chnk_id == chnk_start) {
                chnk_start_target.insert(target);
            }

            if(chnk_id == chnk_end) {
                chnk_end_target.insert(target);
            }
        }
    }

    // some helper variables for async RPC
    std::vector<hermes::mutable_buffer> bufseq{
            hermes::mutable_buffer{const_cast<void*>(buf), write_size},
    };

    // expose user buffers so that they can serve as RDMA data sources
    // (these are automatically "unexposed" when the destructor is called)
    hermes::exposed_memory local_buffers;

    try {
        local_buffers = ld_network_service->expose(
                bufseq, hermes::access_mode::read_only);

    } catch(const std::exception& ex) {
        LOG(ERROR, "Failed to expose buffers for RMA");
        return make_pair(EBUSY, 0);
    }

    std::vector<hermes::rpc_handle<gkfs::rpc::write_data>> handles;

    // Issue non-blocking RPC requests and wait for the result later
    //
    // TODO(amiranda): This could be simplified by adding a vector of inputs
    // to async_engine::broadcast(). This would allow us to avoid manually
    // looping over handles as we do below
    for(const auto& target : targets) {

        // total chunk_size for target
        auto total_chunk_size =
                target_chnks[target].size() * gkfs::config::rpc::chunksize;

        // receiver of first chunk must subtract the offset from first chunk
        if(chnk_start_target.end() != chnk_start_target.find(target)) {
            total_chunk_size -=
                    block_overrun(offset, gkfs::config::rpc::chunksize);
        }

        // receiver of last chunk must subtract
        if(chnk_end_target.end() != chnk_end_target.find(target) &&
           !is_aligned(offset + write_size, gkfs::config::rpc::chunksize)) {
            total_chunk_size -= block_underrun(offset + write_size,
                                               gkfs::config::rpc::chunksize);
        }

        auto endp = CTX->hosts().at(target);

        try {
            LOG(DEBUG, "Sending RPC ...");

            gkfs::rpc::write_data::input in(
                    path,
                    // first offset in targets is the chunk with
                    // a potential offset
                    block_overrun(offset, gkfs::config::rpc::chunksize), target,
                    CTX->hosts().size(),
                    // number of chunks handled by that destination
                    gkfs::rpc::compressBitset(write_ops_vect[target]),
                    target_chnks[target].size(),
                    // chunk start id of this write
                    chnk_start,
                    // chunk end id of this write
                    chnk_end,
                    // total size to write
                    total_chunk_size, local_buffers);

            // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that
            // we can retry for RPC_TRIES (see old commits with margo)
            // TODO(amiranda): hermes will eventually provide a post(endpoint)
            // returning one result and a broadcast(endpoint_set) returning a
            // result_set. When that happens we can remove the .at(0) :/
            handles.emplace_back(
                    ld_network_service->post<gkfs::rpc::write_data>(endp, in));

            LOG(DEBUG,
                "host: {}, path: \"{}\", chunk_start: {}, chunk_end: {}, chunks: {}, size: {}, offset: {}",
                target, path, chnk_start, chnk_end, in.chunk_n(),
                total_chunk_size, in.offset());
        } catch(const std::exception& ex) {
            LOG(ERROR,
                "Unable to send non-blocking rpc for "
                "path \"{}\" [peer: {}]",
                path, target);
            if(num_copies == 0)
                return make_pair(EBUSY, 0);
        }
    }

    auto err = 0;
    ssize_t out_size = 0;
    std::size_t idx = 0;
#ifdef REPLICA_CHECK
    std::vector<uint8_t> fill(chnk_total);
    auto write_ops = write_ops_vect.begin();
#endif
    for(const auto& h : handles) {
        try {
            // XXX We might need a timeout here to not wait forever for an
            // output that never comes?
            auto out = h.get().at(0);

            if(out.err() != 0) {
                LOG(ERROR, "Daemon reported error: {}", out.err());
                err = out.err();
            } else {
                out_size += static_cast<size_t>(out.io_size());
#ifdef REPLICA_CHECK
                if(num_copies) {
                    if(fill.size() == 0) {
                        fill = write_ops->second;
                    } else {
                        for(size_t i = 0; i < fill.size(); i++) {
                            fill[i] |= write_ops->second[i];
                        }
                    }
                }
                write_ops++;
#endif
            }
        } catch(const std::exception& ex) {
            LOG(ERROR, "Failed to get rpc output for path \"{}\" [peer: {}]",
                path, targets[idx]);
            err = EIO;
        }
        idx++;
    }
    // As servers can fail (and we cannot know if the total data is written), we
    // send the updated size but check that at least one copy of all chunks are
    // processed.
    if(num_copies) {
        // A bit-wise or should show that all the chunks are written (255)
        out_size = write_size;
#ifdef REPLICA_CHECK
        for(size_t i = 0; i < fill.size() - 1; i++) {
            if(fill[i] != 255) {
                err = EIO;
                break;
            }
        }
        // Process the leftover bytes
        for(uint64_t chnk_id = (chnk_start + (fill.size() - 1) * 8);
            chnk_id <= chnk_end; chnk_id++) {
            if(!(fill[(chnk_id - chnk_start) / 8] &
                 (1 << ((chnk_id - chnk_start) % 8)))) {
                err = EIO;
                break;
            }
        }
#endif
    }
    /*
     * Typically file systems return the size even if only a part of it was
     * written. In our case, we do not keep track which daemon fully wrote its
     * workload. Thus, we always return size 0 on error.
     */
    if(err)
        return make_pair(err, 0);
    else
        return make_pair(0, out_size);
}

/**
 * Send an RPC request to read to a buffer.
 * @param path
 * @param buf
 * @param offset
 * @param read_size
 * @param num_copies number of copies available (0 is no replication)
 * @param failed nodes failed that should not be used
 * @return pair<error code, read size>
 */
pair<int, ssize_t>
forward_read(const string& path, void* buf, const off64_t offset,
             const size_t read_size, const int8_t num_copies,
             std::set<int8_t>& failed) {

    // import pow2-optimized arithmetic functions
    using namespace gkfs::utils::arithmetic;

    // Calculate chunkid boundaries and numbers so that daemons know in which
    // interval to look for chunks
    auto chnk_start = block_index(offset, gkfs::config::rpc::chunksize);
    auto chnk_end =
            block_index((offset + read_size - 1), gkfs::config::rpc::chunksize);
    auto chnk_total = (chnk_end - chnk_start) + 1;
    // Collect all chunk ids within count that have the same destination so
    // that those are send in one rpc bulk transfer
    std::map<uint64_t, std::vector<uint64_t>> target_chnks{};

    // contains the recipient ids, used to access the target_chnks map.
    // First idx is chunk with potential offset
    std::vector<uint64_t> targets{};
    // targets for the first and last chunk as they need special treatment
    uint64_t chnk_start_target = 0;
    uint64_t chnk_end_target = 0;
    std::unordered_map<uint64_t, std::vector<uint8_t>> read_bitset_vect;

    for(uint64_t chnk_id = chnk_start; chnk_id <= chnk_end; chnk_id++) {
        auto target = CTX->distributor()->locate_data(path, chnk_id, 0);
        if(num_copies > 0) {
            // If we have some failures we select another copy (randomly).
            while(failed.find(target) != failed.end()) {
                LOG(DEBUG, "Selecting another node, target: {} down", target);
                target = CTX->distributor()->locate_data(path, chnk_id,
                                                         rand() % num_copies);
            }
        }

        if(read_bitset_vect.find(target) == read_bitset_vect.end())
            read_bitset_vect[target] =
                    std::vector<uint8_t>(((chnk_total + 7) / 8));
        read_bitset_vect[target][(chnk_id - chnk_start) / 8] |=
                1 << ((chnk_id - chnk_start) % 8); // set

        if(target_chnks.count(target) == 0) {
            target_chnks.insert(
                    std::make_pair(target, std::vector<uint64_t>{chnk_id}));
            targets.push_back(target);
        } else {
            target_chnks[target].push_back(chnk_id);
        }

        // set first and last chnk targets
        if(chnk_id == chnk_start) {
            chnk_start_target = target;
        }

        if(chnk_id == chnk_end) {
            chnk_end_target = target;
        }
    }

    // some helper variables for async RPCs
    std::vector<hermes::mutable_buffer> bufseq{
            hermes::mutable_buffer{buf, read_size},
    };

    // expose user buffers so that they can serve as RDMA data targets
    // (these are automatically "unexposed" when the destructor is called)
    hermes::exposed_memory local_buffers;

    try {
        local_buffers = ld_network_service->expose(
                bufseq, hermes::access_mode::write_only);

    } catch(const std::exception& ex) {
        LOG(ERROR, "Failed to expose buffers for RMA");
        return make_pair(EBUSY, 0);
    }

    std::vector<hermes::rpc_handle<gkfs::rpc::read_data>> handles;

    // Issue non-blocking RPC requests and wait for the result later
    //
    // TODO(amiranda): This could be simplified by adding a vector of inputs
    // to async_engine::broadcast(). This would allow us to avoid manually
    // looping over handles as we do below

    for(const auto& target : targets) {

        // total chunk_size for target
        auto total_chunk_size =
                target_chnks[target].size() * gkfs::config::rpc::chunksize;

        // receiver of first chunk must subtract the offset from first chunk
        if(target == chnk_start_target) {
            total_chunk_size -=
                    block_overrun(offset, gkfs::config::rpc::chunksize);
        }

        // receiver of last chunk must subtract
        if(target == chnk_end_target &&
           !is_aligned(offset + read_size, gkfs::config::rpc::chunksize)) {
            total_chunk_size -= block_underrun(offset + read_size,
                                               gkfs::config::rpc::chunksize);
        }

        auto endp = CTX->hosts().at(target);

        try {

            LOG(DEBUG, "Sending RPC ...");

            gkfs::rpc::read_data::input in(
                    path,
                    // first offset in targets is the chunk with
                    // a potential offset
                    block_overrun(offset, gkfs::config::rpc::chunksize), target,
                    CTX->hosts().size(),
                    gkfs::rpc::compressBitset(read_bitset_vect[target]),
                    // number of chunks handled by that destination
                    target_chnks[target].size(),
                    // chunk start id of this write
                    chnk_start,
                    // chunk end id of this write
                    chnk_end,
                    // total size to write
                    total_chunk_size, local_buffers);

            // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so
            // that we can retry for RPC_TRIES (see old commits with margo)
            // TODO(amiranda): hermes will eventually provide a
            // post(endpoint) returning one result and a
            // broadcast(endpoint_set) returning a result_set. When that
            // happens we can remove the .at(0) :/
            handles.emplace_back(
                    ld_network_service->post<gkfs::rpc::read_data>(endp, in));

            LOG(DEBUG,
                "host: {}, path: {}, chunk_start: {}, chunk_end: {}, chunks: {}, size: {}, offset: {}",
                target, path, chnk_start, chnk_end, in.chunk_n(),
                total_chunk_size, in.offset());

            LOG(TRACE_READS,
                "read {} host: {}, path: {}, chunk_start: {}, chunk_end: {}",
                CTX->get_hostname(), target, path, chnk_start, chnk_end);


        } catch(const std::exception& ex) {
            LOG(ERROR,
                "Unable to send non-blocking rpc for path \"{}\" "
                "[peer: {}]",
                path, target);
            return make_pair(EBUSY, 0);
        }
    }

    // Wait for RPC responses and then get response and add it to out_size
    // which is the read size. All potential outputs are served to free
    // resources regardless of errors, although an errorcode is set.
    auto err = 0;
    ssize_t out_size = 0;
    std::size_t idx = 0;

    for(const auto& h : handles) {
        try {
            // XXX We might need a timeout here to not wait forever for an
            // output that never comes?
            auto out = h.get().at(0);

            if(out.err() != 0) {
                LOG(ERROR, "Daemon reported error: {}", out.err());
                err = out.err();
            }

            out_size += static_cast<size_t>(out.io_size());

        } catch(const std::exception& ex) {
            LOG(ERROR, "Failed to get rpc output for path \"{}\" [peer: {}]",
                path, targets[idx]);
            err = EIO;
            // We should get targets[idx] and remove from the list of peers
            failed.insert(targets[idx]);
            // Then repeat the read with another peer (We repear the full
            // read, this can be optimised but it is a cornercase)
        }
        idx++;
    }


    /*
     * Typically file systems return the size even if only a part of it was
     * read. In our case, we do not keep track which daemon fully read its
     * workload. Thus, we always return size 0 on error.
     */
    if(err)
        return make_pair(err, 0);
    else
        return make_pair(0, out_size);
}

/**
 * Send an RPC request to truncate a file to given new size
 * @param path
 * @param current_size
 * @param new_size
 * @param num_copies Number of replicas
 * @return error code
 */
int
forward_truncate(const std::string& path, size_t current_size, size_t new_size,
                 const int8_t num_copies) {

    // import pow2-optimized arithmetic functions
    using namespace gkfs::utils::arithmetic;

    assert(current_size > new_size);

    // Find out which data servers need to delete data chunks in order to
    // contact only them
    const unsigned int chunk_start =
            block_index(new_size, gkfs::config::rpc::chunksize);
    const unsigned int chunk_end = block_index(current_size - new_size - 1,
                                               gkfs::config::rpc::chunksize);

    std::unordered_set<unsigned int> hosts;
    for(unsigned int chunk_id = chunk_start; chunk_id <= chunk_end;
        ++chunk_id) {
        for(auto copy = 0; copy < (num_copies + 1); ++copy) {
            hosts.insert(CTX->distributor()->locate_data(path, chunk_id, copy));
        }
    }

    std::vector<hermes::rpc_handle<gkfs::rpc::trunc_data>> handles;

    auto err = 0;

    for(const auto& host : hosts) {

        auto endp = CTX->hosts().at(host);

        try {
            LOG(DEBUG, "Sending RPC ...");

            gkfs::rpc::trunc_data::input in(path, new_size);

            // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so
            // that we can retry for RPC_TRIES (see old commits with margo)
            // TODO(amiranda): hermes will eventually provide a
            // post(endpoint) returning one result and a
            // broadcast(endpoint_set) returning a result_set. When that
            // happens we can remove the .at(0) :/
            handles.emplace_back(
                    ld_network_service->post<gkfs::rpc::trunc_data>(endp, in));

        } catch(const std::exception& ex) {
            // TODO(amiranda): we should cancel all previously posted
            // requests here, unfortunately, Hermes does not support it yet
            // :/
            LOG(ERROR, "Failed to send request to host: {}", host);
            err = EIO;
            break; // We need to gather all responses so we can't return
                   // here
        }
    }

    // Wait for RPC responses and then get response
    for(const auto& h : handles) {
        try {
            // XXX We might need a timeout here to not wait forever for an
            // output that never comes?
            auto out = h.get().at(0);

            if(out.err()) {
                LOG(ERROR, "received error response: {}", out.err());
                err = EIO;
            }
        } catch(const std::exception& ex) {
            LOG(ERROR, "while getting rpc output");
            err = EIO;
        }
    }
    return err ? err : 0;
}

/**
 * Send an RPC request to chunk stat all hosts
 * @return pair<error code, rpc::ChunkStat>
 */
pair<int, ChunkStat>
forward_get_chunk_stat() {

    std::vector<hermes::rpc_handle<gkfs::rpc::chunk_stat>> handles;

    auto err = 0;

    for(const auto& endp : CTX->hosts()) {
        try {
            LOG(DEBUG, "Sending RPC to host: {}", endp.to_string());

            gkfs::rpc::chunk_stat::input in(0);

            // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so
            // that we can retry for RPC_TRIES (see old commits with margo)
            // TODO(amiranda): hermes will eventually provide a
            // post(endpoint) returning one result and a
            // broadcast(endpoint_set) returning a result_set. When that
            // happens we can remove the .at(0) :/
            handles.emplace_back(
                    ld_network_service->post<gkfs::rpc::chunk_stat>(endp, in));

        } catch(const std::exception& ex) {
            // TODO(amiranda): we should cancel all previously posted
            // requests here, unfortunately, Hermes does not support it yet
            // :/
            LOG(ERROR, "Failed to send request to host: {}", endp.to_string());
            err = EBUSY;
            break; // We need to gather all responses so we can't return
                   // here
        }
    }

    unsigned long chunk_size = gkfs::config::rpc::chunksize;
    unsigned long chunk_total = 0;
    unsigned long chunk_free = 0;

    // wait for RPC responses
    for(std::size_t i = 0; i < handles.size(); ++i) {

        gkfs::rpc::chunk_stat::output out{};

        try {
            // XXX We might need a timeout here to not wait forever for an
            // output that never comes?
            out = handles[i].get().at(0);

            if(out.err()) {
                err = out.err();
                LOG(ERROR,
                    "Host '{}' reported err code '{}' during stat chunk.",
                    CTX->hosts().at(i).to_string(), err);
                // we don't break here to ensure all responses are processed
                continue;
            }
            assert(out.chunk_size() == chunk_size);
            chunk_total += out.chunk_total();
            chunk_free += out.chunk_free();
        } catch(const std::exception& ex) {
            LOG(ERROR, "Failed to get RPC output from host: {}", i);
            // Avoid setting err if a server fails.
            // err = EBUSY;
        }
    }

    if(err)
        return make_pair(err, ChunkStat{});
    else
        return make_pair(0, ChunkStat{chunk_size, chunk_total, chunk_free});
}

} // namespace gkfs::rpc
