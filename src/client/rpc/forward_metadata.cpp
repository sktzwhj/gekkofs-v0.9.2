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

#include <client/rpc/forward_metadata.hpp>
#include <client/preload.hpp>
#include <client/logging.hpp>
#include <client/preload_util.hpp>
#include <client/open_dir.hpp>
#include <client/rpc/rpc_types.hpp>

#include <common/rpc/rpc_util.hpp>
#include <common/rpc/distributor.hpp>
#include <common/rpc/rpc_types.hpp>

using namespace std;

namespace gkfs::rpc {

/*
 * This file includes all metadata RPC calls.
 * NOTE: No errno is defined here!
 */

/**
 * Send an RPC for a create request
 * @param path
 * @param mode
 * @param copy Number of replica to create
 * @return error code
 */
int
forward_create(const std::string& path, const mode_t mode, const int copy) {

    auto endp = CTX->hosts().at(
            CTX->distributor()->locate_file_metadata(path, copy));

    try {
        LOG(DEBUG, "Sending RPC ...");
        // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
        // can retry for RPC_TRIES (see old commits with margo)
        // TODO(amiranda): hermes will eventually provide a post(endpoint)
        // returning one result and a broadcast(endpoint_set) returning a
        // result_set. When that happens we can remove the .at(0) :/
        auto out = ld_network_service->post<gkfs::rpc::create>(endp, path, mode)
                           .get()
                           .at(0);
        LOG(DEBUG, "Got response success: {}", out.err());

        return out.err() ? out.err() : 0;
    } catch(const std::exception& ex) {
        LOG(ERROR, "while getting rpc output");
        return EBUSY;
    }
}

/**
 * Send an RPC for a stat request
 * @param path
 * @param attr
 * @param copy metadata replica to read from
 * @return error code
 */
int
forward_stat(const std::string& path, string& attr, const int copy) {

    auto endp = CTX->hosts().at(
            CTX->distributor()->locate_file_metadata(path, copy));

    try {
        LOG(DEBUG, "Sending RPC ...");
        // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
        // can retry for RPC_TRIES (see old commits with margo)
        // TODO(amiranda): hermes will eventually provide a post(endpoint)
        // returning one result and a broadcast(endpoint_set) returning a
        // result_set. When that happens we can remove the .at(0) :/
        auto out = ld_network_service->post<gkfs::rpc::stat>(endp, path)
                           .get()
                           .at(0);
        LOG(DEBUG, "Got response success: {}", out.err());

        if(out.err())
            return out.err();

        attr = out.db_val();
    } catch(const std::exception& ex) {
        LOG(ERROR, "while getting rpc output");
        return EBUSY;
    }
    return 0;
}

/**
 * Send an RPC for a remove request. This removes metadata and all data chunks
 * possible distributed across many daemons. Optimizations are in place for
 * small files (file_size / chunk_size) < number_of_daemons where no broadcast
 * to all daemons is used to remove all chunks. Otherwise, a broadcast to all
 * daemons is used.
 *
 * This function only attempts data removal if data exists (determined when
 * metadata is removed)
 * @param path
 * @param num_copies Replication scenarios with many replicas
 * @return error code
 */
int
forward_remove(const std::string& path, const int8_t num_copies) {
    int64_t size = 0;
    uint32_t mode = 0;

    for(auto copy = 0; copy < (num_copies + 1); copy++) {
        auto endp = CTX->hosts().at(
                CTX->distributor()->locate_file_metadata(path, copy));

        /*
         * Send one RPC to metadata destination and remove metadata while
         * retrieving size and mode to determine if data needs to removed too
         */
        try {
            LOG(DEBUG, "Sending RPC ...");
            // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that
            // we can retry for RPC_TRIES (see old commits with margo)
            // TODO(amiranda): hermes will eventually provide a post(endpoint)
            // returning one result and a broadcast(endpoint_set) returning a
            // result_set. When that happens we can remove the .at(0) :/
            auto out = ld_network_service
                               ->post<gkfs::rpc::remove_metadata>(endp, path)
                               .get()
                               .at(0);

            LOG(DEBUG, "Got response success: {}", out.err());

            if(out.err())
                return out.err();
            size = out.size();
            mode = out.mode();
        } catch(const std::exception& ex) {
            LOG(ERROR, "while getting rpc output");
            return EBUSY;
        }
    }
    // if file is not a regular file and it's size is 0, data does not need to
    // be removed, thus, we exit
    if(!(S_ISREG(mode) && (size != 0)))
        return 0;


    std::vector<hermes::rpc_handle<gkfs::rpc::remove_data>> handles;

    // Small files
    if(static_cast<std::size_t>(size / gkfs::config::rpc::chunksize) <
       CTX->hosts().size()) {
        for(auto copymd = 0; copymd < (num_copies + 1); copymd++) {
            const auto metadata_host_id =
                    CTX->distributor()->locate_file_metadata(path, copymd);
            const auto endp_metadata = CTX->hosts().at(metadata_host_id);

            try {
                LOG(DEBUG, "Sending RPC to host: {}",
                    endp_metadata.to_string());
                gkfs::rpc::remove_data::input in(path);
                handles.emplace_back(
                        ld_network_service->post<gkfs::rpc::remove_data>(
                                endp_metadata, in));

                uint64_t chnk_start = 0;
                uint64_t chnk_end = size / gkfs::config::rpc::chunksize;

                for(uint64_t chnk_id = chnk_start; chnk_id <= chnk_end;
                    chnk_id++) {
                    for(auto copy = 0; copy < (num_copies + 1); copy++) {
                        const auto chnk_host_id =
                                CTX->distributor()->locate_data(path, chnk_id,
                                                                copy);
                        if constexpr(gkfs::config::metadata::
                                             implicit_data_removal) {
                            /*
                             * If the chnk host matches the metadata host the
                             * remove request as already been sent as part of
                             * the metadata remove request.
                             */
                            if(chnk_host_id == metadata_host_id)
                                continue;
                        }
                        const auto endp_chnk = CTX->hosts().at(chnk_host_id);

                        LOG(DEBUG, "Sending RPC to host: {}",
                            endp_chnk.to_string());

                        handles.emplace_back(
                                ld_network_service
                                        ->post<gkfs::rpc::remove_data>(
                                                endp_chnk, in));
                    }
                }
            } catch(const std::exception& ex) {
                LOG(ERROR,
                    "Failed to forward non-blocking rpc request reduced remove requests");
                return EBUSY;
            }
        }
    } else { // "Big" files
        for(const auto& endp : CTX->hosts()) {
            try {
                LOG(DEBUG, "Sending RPC to host: {}", endp.to_string());

                gkfs::rpc::remove_data::input in(path);

                // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so
                // that we can retry for RPC_TRIES (see old commits with margo)
                // TODO(amiranda): hermes will eventually provide a
                // post(endpoint) returning one result and a
                // broadcast(endpoint_set) returning a result_set. When that
                // happens we can remove the .at(0) :/

                handles.emplace_back(
                        ld_network_service->post<gkfs::rpc::remove_data>(endp,
                                                                         in));

            } catch(const std::exception& ex) {
                // TODO(amiranda): we should cancel all previously posted
                // requests here, unfortunately, Hermes does not support it yet
                // :/
                LOG(ERROR,
                    "Failed to forward non-blocking rpc request to host: {}",
                    endp.to_string());
                return EBUSY;
            }
        }
    }
    // wait for RPC responses
    auto err = 0;
    for(const auto& h : handles) {
        try {
            // XXX We might need a timeout here to not wait forever for an
            // output that never comes?
            auto out = h.get().at(0);

            if(out.err() != 0) {
                LOG(ERROR, "received error response: {}", out.err());
                err = out.err();
            }
        } catch(const std::exception& ex) {
            LOG(ERROR, "while getting rpc output");
            err = EBUSY;
        }
    }
    return err;
}

/**
 * Send an RPC for a decrement file size request. This is for example used
 * during a truncate() call.
 * @param path
 * @param length
 * @param copy Target replica (0 original)
 * @return error code
 */
int
forward_decr_size(const std::string& path, size_t length, const int copy) {

    auto endp = CTX->hosts().at(
            CTX->distributor()->locate_file_metadata(path, copy));

    try {
        LOG(DEBUG, "Sending RPC ...");
        // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
        // can retry for RPC_TRIES (see old commits with margo)
        // TODO(amiranda): hermes will eventually provide a post(endpoint)
        // returning one result and a broadcast(endpoint_set) returning a
        // result_set. When that happens we can remove the .at(0) :/
        auto out = ld_network_service
                           ->post<gkfs::rpc::decr_size>(endp, path, length)
                           .get()
                           .at(0);

        LOG(DEBUG, "Got response success: {}", out.err());

        return out.err() ? out.err() : 0;
    } catch(const std::exception& ex) {
        LOG(ERROR, "while getting rpc output");
        return EBUSY;
    }
}


/**
 * Send an RPC for an update metadentry request.
 * NOTE: Currently unused.
 * @param path
 * @param md
 * @param md_flags
 * @param copy Target replica (0 original)
 * @return error code
 */
int
forward_update_metadentry(const string& path,
                          const gkfs::metadata::Metadata& md,
                          const gkfs::metadata::MetadentryUpdateFlags& md_flags,
                          const int copy) {

    auto endp = CTX->hosts().at(
            CTX->distributor()->locate_file_metadata(path, copy));

    try {
        LOG(DEBUG, "Sending RPC ...");
        // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
        // can retry for RPC_TRIES (see old commits with margo)
        // TODO(amiranda): hermes will eventually provide a post(endpoint)
        // returning one result and a broadcast(endpoint_set) returning a
        // result_set. When that happens we can remove the .at(0) :/
        auto out = ld_network_service
                           ->post<gkfs::rpc::update_metadentry>(
                                   endp, path,
                                   (md_flags.link_count ? md.link_count() : 0),
                                   /* mode */ 0,
                                   /* uid */ 0,
                                   /* gid */ 0, (md_flags.size ? md.size() : 0),
                                   (md_flags.blocks ? md.blocks() : 0),
                                   (md_flags.atime ? md.atime() : 0),
                                   (md_flags.mtime ? md.mtime() : 0),
                                   (md_flags.ctime ? md.ctime() : 0),
                                   bool_to_merc_bool(md_flags.link_count),
                                   /* mode_flag */ false,
                                   bool_to_merc_bool(md_flags.size),
                                   bool_to_merc_bool(md_flags.blocks),
                                   bool_to_merc_bool(md_flags.atime),
                                   bool_to_merc_bool(md_flags.mtime),
                                   bool_to_merc_bool(md_flags.ctime))
                           .get()
                           .at(0);

        LOG(DEBUG, "Got response success: {}", out.err());

        return out.err() ? out.err() : 0;
    } catch(const std::exception& ex) {
        LOG(ERROR, "while getting rpc output");
        return EBUSY;
    }
}

#ifdef HAS_RENAME
/**
 * Send an RPC for a rename metadentry request.
 * Steps.. SetUp a blkcnt of -1
 * This marks that this file doesn't have to be accessed directly
 * Create a new md with the new name, which should have as value the old name
 * All operations should check blockcnt and extract a NOTEXISTS
 * The operations does not support replication
 * @param oldpath
 * @param newpath
 * @param md
 *
 * @return error code
 */
int
forward_rename(const string& oldpath, const string& newpath,
               const gkfs::metadata::Metadata& md) {

    auto endp = CTX->hosts().at(
            CTX->distributor()->locate_file_metadata(oldpath, 0));

    try {
        LOG(DEBUG, "Sending RPC ...");
        // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
        // can retry for RPC_TRIES (see old commits with margo)
        // TODO(amiranda): hermes will eventually provide a post(endpoint)
        // returning one result and a broadcast(endpoint_set) returning a
        // result_set. When that happens we can remove the .at(0) :/
        auto out = ld_network_service
                           ->post<gkfs::rpc::update_metadentry>(
                                   endp, oldpath, (md.link_count()),
                                   /* mode */ 0,
                                   /* uid */ 0,
                                   /* gid */ 0, md.size(),
                                   /*  blockcnt  */ -1, (md.atime()),
                                   (md.mtime()), (md.ctime()),
                                   bool_to_merc_bool(md.link_count()),
                                   /* mode_flag */ false,
                                   bool_to_merc_bool(md.size()), 1,
                                   bool_to_merc_bool(md.atime()),
                                   bool_to_merc_bool(md.mtime()),
                                   bool_to_merc_bool(md.ctime()))
                           .get()
                           .at(0);

        LOG(DEBUG, "Got response success: {}", out.err());

        // Now create the new file

    } catch(const std::exception& ex) {
        LOG(ERROR, "while getting rpc output");
        return EBUSY;
    }

    auto md2 = md;

    md2.target_path(oldpath);
    /*
     * Now create the new file
     */
    // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
    // can retry for RPC_TRIES (see old commits with margo)
    // TODO(amiranda): hermes will eventually provide a post(endpoint)
    // returning one result and a broadcast(endpoint_set) returning a
    // result_set. When that happens we can remove the .at(0) :/
    auto endp2 = CTX->hosts().at(
            CTX->distributor()->locate_file_metadata(newpath, 0));

    try {
        LOG(DEBUG, "Sending RPC ...");
        // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
        // can retry for RPC_TRIES (see old commits with margo)
        // TODO(amiranda): hermes will eventually provide a post(endpoint)
        // returning one result and a broadcast(endpoint_set) returning a
        // result_set. When that happens we can remove the .at(0) :/

        auto out = ld_network_service
                           ->post<gkfs::rpc::create>(endp2, newpath, md2.mode())
                           .get()
                           .at(0);
        LOG(DEBUG, "Got response success: {}", out.err());

    } catch(const std::exception& ex) {
        LOG(ERROR, "while getting rpc output");
        return EBUSY;
    }


    try {
        LOG(DEBUG, "Sending RPC ...");
        // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
        // can retry for RPC_TRIES (see old commits with margo)
        // TODO(amiranda): hermes will eventually provide a post(endpoint)
        // returning one result and a broadcast(endpoint_set) returning a
        // result_set. When that happens we can remove the .at(0) :/
        // Update new file with target link = oldpath
        auto out =
                ld_network_service
                        ->post<gkfs::rpc::mk_symlink>(endp2, newpath, oldpath)
                        .get()
                        .at(0);

        LOG(DEBUG, "Got response success: {}", out.err());

        // return out.err() ? out.err() : 0;

    } catch(const std::exception& ex) {
        LOG(ERROR, "while getting rpc output");
        return EBUSY;
    }

    // Update the renamed path to solve the issue with fstat with fd)
    try {
        LOG(DEBUG, "Sending RPC ...");
        // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
        // can retry for RPC_TRIES (see old commits with margo)
        // TODO(amiranda): hermes will eventually provide a post(endpoint)
        // returning one result and a broadcast(endpoint_set) returning a
        // result_set. When that happens we can remove the .at(0) :/
        auto out = ld_network_service
                           ->post<gkfs::rpc::mk_symlink>(endp, oldpath, newpath)
                           .get()
                           .at(0);

        LOG(DEBUG, "Got response success: {}", out.err());

        // return out.err() ? out.err() : 0;
        return 0;
    } catch(const std::exception& ex) {
        LOG(ERROR, "while getting rpc output");
        return EBUSY;
    }
}

#endif

/**
 * Send an RPC request for an update to the file size.
 * This is called during a write() call or similar
 * A single correct call is needed only to progress.
 * @param path
 * @param size
 * @param offset
 * @param append_flag
 * @param num_copies number of replicas
 * @return pair<error code, size after update>
 */
pair<int, off64_t>
forward_update_metadentry_size(const string& path, const size_t size,
                               const off64_t offset, const bool append_flag,
                               const int num_copies) {

    std::vector<hermes::rpc_handle<gkfs::rpc::update_metadentry_size>> handles;

    for(auto copy = 0; copy < num_copies + 1; copy++) {
        auto endp = CTX->hosts().at(
                CTX->distributor()->locate_file_metadata(path, copy));
        try {
            LOG(DEBUG, "Sending RPC ...");
            // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that
            // we can retry for RPC_TRIES (see old commits with margo)
            // TODO(amiranda): hermes will eventually provide a post(endpoint)
            // returning one result and a broadcast(endpoint_set) returning a
            // result_set. When that happens we can remove the .at(0) :/
            handles.emplace_back(
                    ld_network_service->post<gkfs::rpc::update_metadentry_size>(
                            endp, path, size, offset,
                            bool_to_merc_bool(append_flag)));
        } catch(const std::exception& ex) {
            LOG(ERROR, "while getting rpc output");
            return make_pair(EBUSY, 0);
        }
    }
    auto err = 0;
    ssize_t out_size = 0;
    auto idx = 0;
    bool valid = false;
    for(const auto& h : handles) {
        try {
            // XXX We might need a timeout here to not wait forever for an
            // output that never comes?
            auto out = h.get().at(0);

            if(out.err() != 0) {
                LOG(ERROR, "Daemon {} reported error: {}", idx, out.err());
            } else {
                valid = true;
                out_size = out.ret_size();
            }

        } catch(const std::exception& ex) {
            LOG(ERROR, "Failed to get rpc output");
            if(!valid) {
                err = EIO;
            }
        }
        idx++;
    }

    if(!valid)
        return make_pair(err, 0);
    else
        return make_pair(0, out_size);
}


/**
 * Send an RPC request to get the current file size.
 * This is called during a lseek() call
 * @param path
 * @param copy Target replica (0 original)
 * @return pair<error code, file size>
 */
pair<int, off64_t>
forward_get_metadentry_size(const std::string& path, const int copy) {

    auto endp = CTX->hosts().at(
            CTX->distributor()->locate_file_metadata(path, copy));

    try {
        LOG(DEBUG, "Sending RPC ...");
        // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
        // can retry for RPC_TRIES (see old commits with margo)
        // TODO(amiranda): hermes will eventually provide a post(endpoint)
        // returning one result and a broadcast(endpoint_set) returning a
        // result_set. When that happens we can remove the .at(0) :/
        auto out = ld_network_service
                           ->post<gkfs::rpc::get_metadentry_size>(endp, path)
                           .get()
                           .at(0);

        LOG(DEBUG, "Got response success: {}", out.err());

        if(out.err())
            return make_pair(out.err(), 0);
        else
            return make_pair(0, out.ret_size());
    } catch(const std::exception& ex) {
        LOG(ERROR, "while getting rpc output");
        return make_pair(EBUSY, 0);
    }
}

/**
 * Send an RPC request to receive all entries of a directory.
 * @param open_dir
 * @return error code
 */
pair<int, shared_ptr<gkfs::filemap::OpenDir>>
forward_get_dirents(const string& path) {

    LOG(DEBUG, "{}() enter for path '{}'", __func__, path)

    auto const targets = CTX->distributor()->locate_directory_metadata(path);

    /* preallocate receiving buffer. The actual size is not known yet.
     *
     * On C++14 make_unique function also zeroes the newly allocated buffer.
     * It turns out that this operation is increadibly slow for such a big
     * buffer. Moreover we don't need a zeroed buffer here.
     */
    auto large_buffer = std::unique_ptr<char[]>(
            new char[gkfs::config::rpc::dirents_buff_size]);

    // XXX there is a rounding error here depending on the number of targets...
    const std::size_t per_host_buff_size =
            gkfs::config::rpc::dirents_buff_size / targets.size();

    // expose local buffers for RMA from servers
    std::vector<hermes::exposed_memory> exposed_buffers;
    exposed_buffers.reserve(targets.size());

    for(std::size_t i = 0; i < targets.size(); ++i) {
        try {
            exposed_buffers.emplace_back(ld_network_service->expose(
                    std::vector<hermes::mutable_buffer>{hermes::mutable_buffer{
                            large_buffer.get() + (i * per_host_buff_size),
                            per_host_buff_size}},
                    hermes::access_mode::write_only));
        } catch(const std::exception& ex) {
            LOG(ERROR, "{}() Failed to expose buffers for RMA. err '{}'",
                __func__, ex.what());
            return make_pair(EBUSY, nullptr);
        }
    }

    auto err = 0;
    // send RPCs
    std::vector<hermes::rpc_handle<gkfs::rpc::get_dirents>> handles;

    for(std::size_t i = 0; i < targets.size(); ++i) {

        // Setup rpc input parameters for each host
        auto endp = CTX->hosts().at(targets[i]);

        gkfs::rpc::get_dirents::input in(path, exposed_buffers[i]);

        try {
            LOG(DEBUG, "{}() Sending RPC to host: '{}'", __func__, targets[i]);
            handles.emplace_back(
                    ld_network_service->post<gkfs::rpc::get_dirents>(endp, in));
        } catch(const std::exception& ex) {
            LOG(ERROR,
                "{}() Unable to send non-blocking get_dirents() on {} [peer: {}] err '{}'",
                __func__, path, targets[i], ex.what());
            err = EBUSY;
            break; // we need to gather responses from already sent RPCS
        }
    }

    LOG(DEBUG,
        "{}() path '{}' send rpc_srv_get_dirents() rpc to '{}' targets. per_host_buff_size '{}' Waiting on reply next and deserialize",
        __func__, path, targets.size(), per_host_buff_size);

    auto send_error = err != 0;
    auto open_dir = make_shared<gkfs::filemap::OpenDir>(path);
    // wait for RPC responses
    for(std::size_t i = 0; i < handles.size(); ++i) {

        gkfs::rpc::get_dirents::output out;

        try {
            // XXX We might need a timeout here to not wait forever for an
            // output that never comes?
            out = handles[i].get().at(0);
            // skip processing dirent data if there was an error during send
            // In this case all responses are gathered but their contents
            // skipped
            if(send_error)
                continue;

            if(out.err() != 0) {
                LOG(ERROR,
                    "{}() Failed to retrieve dir entries from host '{}'. Error '{}', path '{}'",
                    __func__, targets[i], strerror(out.err()), path);
                err = out.err();
                // We need to gather all responses before exiting
                continue;
            }
        } catch(const std::exception& ex) {
            LOG(ERROR,
                "{}() Failed to get rpc output.. [path: {}, target host: {}] err '{}'",
                __func__, path, targets[i], ex.what());
            err = EBUSY;
            // We need to gather all responses before exiting
            continue;
        }

        // each server wrote information to its pre-defined region in
        // large_buffer, recover it by computing the base_address for each
        // particular server and adding the appropriate offsets
        assert(exposed_buffers[i].count() == 1);
        void* base_ptr = exposed_buffers[i].begin()->data();

        bool* bool_ptr = reinterpret_cast<bool*>(base_ptr);
        char* names_ptr = reinterpret_cast<char*>(base_ptr) +
                          (out.dirents_size() * sizeof(bool));

        for(std::size_t j = 0; j < out.dirents_size(); j++) {

            gkfs::filemap::FileType ftype =
                    (*bool_ptr) ? gkfs::filemap::FileType::directory
                                : gkfs::filemap::FileType::regular;
            bool_ptr++;

            // Check that we are not outside the recv_buff for this specific
            // host
            assert((names_ptr - reinterpret_cast<char*>(base_ptr)) > 0);
            assert(static_cast<unsigned long int>(
                           names_ptr - reinterpret_cast<char*>(base_ptr)) <
                   per_host_buff_size);

            auto name = std::string(names_ptr);
            // number of characters in entry + \0 terminator
            names_ptr += name.size() + 1;

            open_dir->add(name, ftype);
        }
    }
    return make_pair(err, open_dir);
}

/**
 * Send an RPC request to receive all entries of a directory in a server.
 * @param path
 * @param server
 * @return error code
 * Returns a tuple with path-isdir-size and ctime
 * We still use dirents_buff_size, but we need to match in the client side, as
 * the buffer is provided by the "gfind" applications However, as we only ask
 * for a server the size should be enought for most of the scenarios. We are
 * reusing the forward_get_dirents code. As we only need a server, we could
 * simplify the code removing the asynchronous part.
 */
pair<int, vector<tuple<const std::string, bool, size_t, time_t>>>
forward_get_dirents_single(const string& path, int server) {

    LOG(DEBUG, "{}() enter for path '{}'", __func__, path)

    auto const targets = CTX->distributor()->locate_directory_metadata(path);

    /* preallocate receiving buffer. The actual size is not known yet.
     *
     * On C++14 make_unique function also zeroes the newly allocated buffer.
     * It turns out that this operation is increadibly slow for such a big
     * buffer. Moreover we don't need a zeroed buffer here.
     */
    auto large_buffer = std::unique_ptr<char[]>(
            new char[gkfs::config::rpc::dirents_buff_size]);

    // We use the full size per server...
    const std::size_t per_host_buff_size = gkfs::config::rpc::dirents_buff_size;
    vector<tuple<const std::string, bool, size_t, time_t>> output;

    // expose local buffers for RMA from servers
    std::vector<hermes::exposed_memory> exposed_buffers;
    exposed_buffers.reserve(1);
    std::size_t i = server;
    try {
        exposed_buffers.emplace_back(ld_network_service->expose(
                std::vector<hermes::mutable_buffer>{hermes::mutable_buffer{
                        large_buffer.get(), per_host_buff_size}},
                hermes::access_mode::write_only));
    } catch(const std::exception& ex) {
        LOG(ERROR, "{}() Failed to expose buffers for RMA. err '{}'", __func__,
            ex.what());
        return make_pair(EBUSY, output);
    }

    auto err = 0;
    // send RPCs
    std::vector<hermes::rpc_handle<gkfs::rpc::get_dirents_extended>> handles;

    auto endp = CTX->hosts().at(targets[i]);

    gkfs::rpc::get_dirents_extended::input in(path, exposed_buffers[0]);

    try {
        LOG(DEBUG, "{}() Sending RPC to host: '{}'", __func__, targets[i]);
        handles.emplace_back(
                ld_network_service->post<gkfs::rpc::get_dirents_extended>(endp,
                                                                          in));
    } catch(const std::exception& ex) {
        LOG(ERROR,
            "{}() Unable to send non-blocking get_dirents() on {} [peer: {}] err '{}'",
            __func__, path, targets[i], ex.what());
        err = EBUSY;
    }

    LOG(DEBUG,
        "{}() path '{}' send rpc_srv_get_dirents() rpc to '{}' targets. per_host_buff_size '{}' Waiting on reply next and deserialize",
        __func__, path, targets.size(), per_host_buff_size);

    // wait for RPC responses

    gkfs::rpc::get_dirents_extended::output out;

    try {
        // XXX We might need a timeout here to not wait forever for an
        // output that never comes?
        out = handles[0].get().at(0);
        // skip processing dirent data if there was an error during send
        // In this case all responses are gathered but their contents skipped

        if(out.err() != 0) {
            LOG(ERROR,
                "{}() Failed to retrieve dir entries from host '{}'. Error '{}', path '{}'",
                __func__, targets[0], strerror(out.err()), path);
            err = out.err();
            // We need to gather all responses before exiting
        }
    } catch(const std::exception& ex) {
        LOG(ERROR,
            "{}() Failed to get rpc output.. [path: {}, target host: {}] err '{}'",
            __func__, path, targets[0], ex.what());
        err = EBUSY;
        // We need to gather all responses before exiting
    }

    // The parenthesis is extremely important if not the cast will add as a
    // size_t or a time_t and not as a char
    auto out_buff_ptr = static_cast<char*>(exposed_buffers[0].begin()->data());
    auto bool_ptr = reinterpret_cast<bool*>(out_buff_ptr);
    auto size_ptr = reinterpret_cast<size_t*>(
            (out_buff_ptr) + (out.dirents_size() * sizeof(bool)));
    auto ctime_ptr = reinterpret_cast<time_t*>(
            (out_buff_ptr) +
            (out.dirents_size() * (sizeof(bool) + sizeof(size_t))));
    auto names_ptr =
            out_buff_ptr + (out.dirents_size() *
                            (sizeof(bool) + sizeof(size_t) + sizeof(time_t)));

    for(std::size_t j = 0; j < out.dirents_size(); j++) {

        bool ftype = (*bool_ptr);
        bool_ptr++;

        size_t size = *size_ptr;
        size_ptr++;

        time_t ctime = *ctime_ptr;
        ctime_ptr++;

        auto name = std::string(names_ptr);
        // number of characters in entry + \0 terminator
        names_ptr += name.size() + 1;
        output.emplace_back(std::forward_as_tuple(name, ftype, size, ctime));
    }
    return make_pair(err, output);
}


#ifdef HAS_SYMLINKS

/**
 * Send an RPC request to create a symlink.
 * @param path
 * @param target_path
 * @return error code
 */
int
forward_mk_symlink(const std::string& path, const std::string& target_path) {

    auto endp =
            CTX->hosts().at(CTX->distributor()->locate_file_metadata(path, 0));

    try {
        LOG(DEBUG, "Sending RPC ...");
        // TODO(amiranda): add a post() with RPC_TIMEOUT to hermes so that we
        // can retry for RPC_TRIES (see old commits with margo)
        // TODO(amiranda): hermes will eventually provide a post(endpoint)
        // returning one result and a broadcast(endpoint_set) returning a
        // result_set. When that happens we can remove the .at(0) :/
        auto out =
                ld_network_service
                        ->post<gkfs::rpc::mk_symlink>(endp, path, target_path)
                        .get()
                        .at(0);

        LOG(DEBUG, "Got response success: {}", out.err());

        return out.err() ? out.err() : 0;
    } catch(const std::exception& ex) {
        LOG(ERROR, "while getting rpc output");
        return EBUSY;
    }
}

#endif

} // namespace gkfs::rpc
