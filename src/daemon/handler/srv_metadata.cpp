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
/**
 * @brief Provides all Margo RPC handler definitions called by Mercury on client
 * request for all file system metadata operations.
 * @internal
 * The end of the file defines the associates the Margo RPC handler functions
 * and associates them with their corresponding GekkoFS handler functions.
 * @endinternal
 */

#include <daemon/handler/rpc_defs.hpp>
#include <daemon/handler/rpc_util.hpp>
#include <daemon/backend/metadata/db.hpp>
#include <daemon/backend/data/chunk_storage.hpp>
#include <daemon/ops/metadentry.hpp>

#include <common/rpc/rpc_types.hpp>
#include <common/statistics/stats.hpp>

using namespace std;

namespace {

/**
 * @brief Serves a file/directory create request or returns an error to the
 * client if the object already exists.
 * @internal
 * The create request creates or updates a corresponding entry in the KV store.
 * If the object already exists, the RPC output struct includes an EEXIST error
 * code. This is not a hard error. Other unexpected errors are placed in the
 * output struct as well.
 *
 * All exceptions must be caught here and dealt with accordingly.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_create(hg_handle_t handle) {
    rpc_mk_node_in_t in;
    rpc_err_out_t out;

    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS)
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to retrieve input from handle", __func__);
    assert(ret == HG_SUCCESS);
    GKFS_DATA->spdlogger()->debug("{}() Got RPC with path '{}'", __func__,
                                  in.path);
    gkfs::metadata::Metadata md(in.mode);
    try {
        // create metadentry
        gkfs::metadata::create(in.path, md);
        out.err = 0;
    } catch(const gkfs::metadata::ExistsException& e) {
        out.err = EEXIST;
    } catch(const std::exception& e) {
        GKFS_DATA->spdlogger()->error("{}() Failed to create metadentry: '{}'",
                                      __func__, e.what());
        out.err = -1;
    }

    GKFS_DATA->spdlogger()->debug("{}() Sending output err '{}'", __func__,
                                  out.err);
    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error("{}() Failed to respond", __func__);
    }

    // Destroy handle when finished
    margo_free_input(handle, &in);
    margo_destroy(handle);
    if(GKFS_DATA->enable_stats()) {
        GKFS_DATA->stats()->add_value_iops(
                gkfs::utils::Stats::IopsOp::iops_create);
    }
    return HG_SUCCESS;
}

/**
 * @brief Serves a stat request or returns an error to the
 * client if the object does not exist.
 * @internal
 * The stat request reads the corresponding entry in the KV store. The value
 * string is directly passed to the client. It sets an error code if the object
 * does not exist or in other unexpected errors.
 *
 * All exceptions must be caught here and dealt with accordingly.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_stat(hg_handle_t handle) {
    rpc_path_only_in_t in{};
    rpc_stat_out_t out{};
    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS)
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to retrieve input from handle", __func__);
    assert(ret == HG_SUCCESS);
    GKFS_DATA->spdlogger()->debug("{}() path: '{}'", __func__, in.path);
    std::string val;

    try {
        // get the metadata
        val = gkfs::metadata::get_str(in.path);
        out.db_val = val.c_str();
        out.err = 0;
        GKFS_DATA->spdlogger()->debug("{}() Sending output mode '{}'", __func__,
                                      out.db_val);
    } catch(const gkfs::metadata::NotFoundException& e) {
        GKFS_DATA->spdlogger()->debug("{}() Entry not found: '{}'", __func__,
                                      in.path);
        out.err = ENOENT;
    } catch(const std::exception& e) {
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to get metadentry from DB: '{}'", __func__,
                e.what());
        out.err = EBUSY;
    }

    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error("{}() Failed to respond", __func__);
    }

    // Destroy handle when finished
    margo_free_input(handle, &in);
    margo_destroy(handle);

    if(GKFS_DATA->enable_stats()) {
        GKFS_DATA->stats()->add_value_iops(
                gkfs::utils::Stats::IopsOp::iops_stats);
    }
    return HG_SUCCESS;
}

/**
 * @brief Serves a request to decrease the file size in the object's KV store
 * entry.
 * @internal
 * All exceptions must be caught here and dealt with accordingly. Any errors are
 * placed in the response.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_decr_size(hg_handle_t handle) {
    rpc_trunc_in_t in{};
    rpc_err_out_t out{};

    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to retrieve input from handle", __func__);
        throw runtime_error("Failed to retrieve input from handle");
    }

    GKFS_DATA->spdlogger()->debug("{}() path: '{}', length: '{}'", __func__,
                                  in.path, in.length);

    try {
        GKFS_DATA->mdb()->decrease_size(in.path, in.length);
        out.err = 0;
    } catch(const std::exception& e) {
        GKFS_DATA->spdlogger()->error("{}() Failed to decrease size: '{}'",
                                      __func__, e.what());
        out.err = EIO;
    }

    GKFS_DATA->spdlogger()->debug("{}() Sending output '{}'", __func__,
                                  out.err);
    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error("{}() Failed to respond", __func__);
        throw runtime_error("Failed to respond");
    }
    // Destroy handle when finished
    margo_free_input(handle, &in);
    margo_destroy(handle);
    return HG_SUCCESS;
}

/**
 * @brief Serves a request to remove a file/directory metadata.
 * @internal
 * The handler triggers the removal of the KV store entry but still returns the
 * file mode and size information to the client. This is because the size is
 * needed to remove all data chunks. The metadata is removed first to ensure
 * data isn't removed while the metadata is still available. This could cause
 * issues because a stat request would say that the file still exists.
 *
 * gkfs::config::metadata::implicit_data_removal offers an optimization to
 * implicitly remove the data chunks on the metadata node. This can increase
 * remove performance for small files.
 *
 * All exceptions must be caught here and dealt with accordingly. Any errors are
 * placed in the response.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_remove_metadata(hg_handle_t handle) {
    rpc_rm_node_in_t in{};
    rpc_rm_metadata_out_t out{};

    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS)
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to retrieve input from handle", __func__);
    assert(ret == HG_SUCCESS);
    GKFS_DATA->spdlogger()->debug("{}() Got remove metadata RPC with path '{}'",
                                  __func__, in.path);

    // Remove metadentry if exists on the node
    try {
        auto md = gkfs::metadata::get(in.path);
        gkfs::metadata::remove(in.path);
        out.err = 0;
        out.mode = md.mode();
        out.size = md.size();
        if constexpr(gkfs::config::metadata::implicit_data_removal) {
            if(S_ISREG(md.mode()) && (md.size() != 0))
                GKFS_DATA->storage()->destroy_chunk_space(in.path);
        }

    } catch(const gkfs::metadata::DBException& e) {
        GKFS_DATA->spdlogger()->error("{}(): path '{}' message '{}'", __func__,
                                      in.path, e.what());
        out.err = EIO;
    } catch(const gkfs::data::ChunkStorageException& e) {
        GKFS_DATA->spdlogger()->error(
                "{}(): path '{}' errcode '{}' message '{}'", __func__, in.path,
                e.code().value(), e.what());
        out.err = e.code().value();
    } catch(const std::exception& e) {
        GKFS_DATA->spdlogger()->error("{}() path '{}' message '{}'", __func__,
                                      in.path, e.what());
        out.err = EBUSY;
    }

    GKFS_DATA->spdlogger()->debug("{}() Sending output '{}'", __func__,
                                  out.err);
    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error("{}() Failed to respond", __func__);
    }
    // Destroy handle when finished
    margo_free_input(handle, &in);
    margo_destroy(handle);
    if(GKFS_DATA->enable_stats()) {
        GKFS_DATA->stats()->add_value_iops(
                gkfs::utils::Stats::IopsOp::iops_remove);
    }
    return HG_SUCCESS;
}

/**
 * @brief Serves a request to remove all file data chunks on this daemon.
 * @internal
 * The handler simply issues the removal of all chunk files on the local file
 * system.
 *
 * All exceptions must be caught here and dealt with accordingly. Any errors are
 * placed in the response.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_remove_data(hg_handle_t handle) {
    rpc_rm_node_in_t in{};
    rpc_err_out_t out{};

    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS)
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to retrieve input from handle", __func__);
    assert(ret == HG_SUCCESS);
    GKFS_DATA->spdlogger()->debug("{}() Got remove data RPC with path '{}'",
                                  __func__, in.path);

    // Remove all chunks for that file
    try {
        GKFS_DATA->storage()->destroy_chunk_space(in.path);
        out.err = 0;
    } catch(const gkfs::data::ChunkStorageException& e) {
        GKFS_DATA->spdlogger()->error(
                "{}(): path '{}' errcode '{}' message '{}'", __func__, in.path,
                e.code().value(), e.what());
        out.err = e.code().value();
    } catch(const std::exception& e) {
        GKFS_DATA->spdlogger()->error("{}() path '{}' message '{}'", __func__,
                                      in.path, e.what());
        out.err = EBUSY;
    }

    GKFS_DATA->spdlogger()->debug("{}() Sending output '{}'", __func__,
                                  out.err);
    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error("{}() Failed to respond", __func__);
    }
    // Destroy handle when finished
    margo_free_input(handle, &in);
    margo_destroy(handle);
    return HG_SUCCESS;
}

/**
 * @brief Serves a request to update the metadata. This function is UNUSED.
 * @internal
 * All exceptions must be caught here and dealt with accordingly. Any errors are
 * placed in the response.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_update_metadentry(hg_handle_t handle) {
    // Note: Currently this handler is not called by the client.
    rpc_update_metadentry_in_t in{};
    rpc_err_out_t out{};


    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS)
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to retrieve input from handle", __func__);
    assert(ret == HG_SUCCESS);
    GKFS_DATA->spdlogger()->debug(
            "{}() Got update metadentry RPC with path '{}'", __func__, in.path);

    // do update
    try {
        gkfs::metadata::Metadata md = gkfs::metadata::get(in.path);
        if(in.block_flag == HG_TRUE)
            md.blocks(in.blocks);
        if(in.nlink_flag == HG_TRUE)
            md.link_count(in.nlink);
        if(in.size_flag == HG_TRUE)
            md.size(in.size);
        if(in.atime_flag == HG_TRUE)
            md.atime(in.atime);
        if(in.mtime_flag == HG_TRUE)
            md.mtime(in.mtime);
        if(in.ctime_flag == HG_TRUE)
            md.ctime(in.ctime);
        gkfs::metadata::update(in.path, md);
        out.err = 0;
    } catch(const std::exception& e) {
        // TODO handle NotFoundException
        GKFS_DATA->spdlogger()->error("{}() Failed to update entry", __func__);
        out.err = 1;
    }

    GKFS_DATA->spdlogger()->debug("{}() Sending output '{}'", __func__,
                                  out.err);
    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error("{}() Failed to respond", __func__);
    }

    // Destroy handle when finished
    margo_free_input(handle, &in);
    margo_destroy(handle);
    return HG_SUCCESS;
}

/**
 * @brief Serves a request to update the file size to a given value in the KV
 * store.
 * @internal
 * All exceptions must be caught here and dealt with accordingly. Any errors are
 * placed in the response.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_update_metadentry_size(hg_handle_t handle) {
    rpc_update_metadentry_size_in_t in{};
    rpc_update_metadentry_size_out_t out{};

    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS)
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to retrieve input from handle", __func__);
    assert(ret == HG_SUCCESS);
    GKFS_DATA->spdlogger()->debug(
            "{}() path: '{}', size: '{}', offset: '{}', append: '{}'", __func__,
            in.path, in.size, in.offset, in.append);

    try {
        out.ret_offset = gkfs::metadata::update_size(
                in.path, in.size, in.offset, (in.append == HG_TRUE));
        out.err = 0;
    } catch(const gkfs::metadata::NotFoundException& e) {
        GKFS_DATA->spdlogger()->debug("{}() Entry not found: '{}'", __func__,
                                      in.path);
        out.err = ENOENT;
    } catch(const std::exception& e) {
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to update metadentry size on DB: '{}'", __func__,
                e.what());
        out.err = EBUSY;
    }

    GKFS_DATA->spdlogger()->debug("{}() Sending output '{}'", __func__,
                                  out.err);
    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error("{}() Failed to respond", __func__);
    }

    // Destroy handle when finished
    margo_free_input(handle, &in);
    margo_destroy(handle);
    return HG_SUCCESS;
}

/**
 * @brief Serves a request to return the current file size.
 * @internal
 * All exceptions must be caught here and dealt with accordingly. Any errors are
 * placed in the response.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_get_metadentry_size(hg_handle_t handle) {
    rpc_path_only_in_t in{};
    rpc_get_metadentry_size_out_t out{};


    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS)
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to retrieve input from handle", __func__);
    assert(ret == HG_SUCCESS);
    GKFS_DATA->spdlogger()->debug(
            "{}() Got update metadentry size RPC with path '{}'", __func__,
            in.path);

    // do update
    try {
        out.ret_size = gkfs::metadata::get_size(in.path);
        out.err = 0;
    } catch(const gkfs::metadata::NotFoundException& e) {
        GKFS_DATA->spdlogger()->debug("{}() Entry not found: '{}'", __func__,
                                      in.path);
        out.err = ENOENT;
    } catch(const std::exception& e) {
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to get metadentry size from DB: '{}'", __func__,
                e.what());
        out.err = EBUSY;
    }

    GKFS_DATA->spdlogger()->debug("{}() Sending output '{}'", __func__,
                                  out.err);
    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error("{}() Failed to respond", __func__);
    }

    // Destroy handle when finished
    margo_free_input(handle, &in);
    margo_destroy(handle);
    return HG_SUCCESS;
}

/**
 * @brief Serves a request to return all file system objects in a directory.
 * @internal
 * This handler triggers a KV store scan starting at the given path prefix that
 * represents a directory. All KV store entries are returned via a bulk transfer
 * as it can involve an arbitrary number of entries.
 *
 * Note, the bulk buffer size is decided by the client statically although it
 * doesn't know if it the space is sufficient to accomodate all entries. This is
 * planned to be fixed in the future.
 *
 * All exceptions must be caught here and dealt with accordingly. Any errors are
 * placed in the response.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_get_dirents(hg_handle_t handle) {
    rpc_get_dirents_in_t in{};
    rpc_get_dirents_out_t out{};
    out.err = EIO;
    out.dirents_size = 0;
    hg_bulk_t bulk_handle = nullptr;

    // Get input parmeters
    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error(
                "{}() Could not get RPC input data with err '{}'", __func__,
                ret);
        out.err = EBUSY;
        return gkfs::rpc::cleanup_respond(&handle, &in, &out);
    }

    // Retrieve size of source buffer
    auto hgi = margo_get_info(handle);
    auto mid = margo_hg_info_get_instance(hgi);
    auto bulk_size = margo_bulk_get_size(in.bulk_handle);
    GKFS_DATA->spdlogger()->debug("{}() Got RPC: path '{}' bulk_size '{}' ",
                                  __func__, in.path, bulk_size);

    // Get directory entries from local DB
    vector<pair<string, bool>> entries{};
    try {
        entries = gkfs::metadata::get_dirents(in.path);
    } catch(const ::exception& e) {
        GKFS_DATA->spdlogger()->error("{}() Error during get_dirents(): '{}'",
                                      __func__, e.what());
        return gkfs::rpc::cleanup_respond(&handle, &in, &out);
    }

    GKFS_DATA->spdlogger()->trace(
            "{}() path '{}' Read database with '{}' entries", __func__, in.path,
            entries.size());

    if(entries.empty()) {
        out.err = 0;
        return gkfs::rpc::cleanup_respond(&handle, &in, &out);
    }

    // Calculate total output size
    // TODO OPTIMIZATION: this can be calculated inside db_get_dirents
    size_t tot_names_size = 0;
    for(auto const& e : entries) {
        tot_names_size += e.first.size();
    }

    // tot_names_size (# characters in entry) + # entries * (bool size + char
    // size for \0 character)
    size_t out_size =
            tot_names_size + entries.size() * (sizeof(bool) + sizeof(char));
    if(bulk_size < out_size) {
        // Source buffer is smaller than total output size
        GKFS_DATA->spdlogger()->error(
                "{}() Entries do not fit source buffer. bulk_size '{}' < out_size '{}' must be satisfied!",
                __func__, bulk_size, out_size);
        out.err = ENOBUFS;
        return gkfs::rpc::cleanup_respond(&handle, &in, &out);
    }

    void* bulk_buf; // buffer for bulk transfer
    // create bulk handle and allocated memory for buffer with out_size
    // information
    ret = margo_bulk_create(mid, 1, nullptr, &out_size, HG_BULK_READ_ONLY,
                            &bulk_handle);
    if(ret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error("{}() Failed to create bulk handle",
                                      __func__);
        return gkfs::rpc::cleanup_respond(&handle, &in, &out, &bulk_handle);
    }
    // access the internally allocated memory buffer and put it into bulk_buf
    uint32_t actual_count; // number of segments. we use one here because we
                           // push the whole buffer at once
    ret = margo_bulk_access(bulk_handle, 0, out_size, HG_BULK_READ_ONLY, 1,
                            &bulk_buf, &out_size, &actual_count);
    if(ret != HG_SUCCESS || actual_count != 1) {
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to access allocated buffer from bulk handle",
                __func__);
        return gkfs::rpc::cleanup_respond(&handle, &in, &out, &bulk_handle);
    }

    GKFS_DATA->spdlogger()->trace(
            "{}() path '{}' entries '{}' out_size '{}'. Set up local read only bulk handle and allocated buffer with size '{}'",
            __func__, in.path, entries.size(), out_size, out_size);

    // Serialize output data on local buffer
    auto out_buff_ptr = static_cast<char*>(bulk_buf);
    auto bool_ptr = reinterpret_cast<bool*>(out_buff_ptr);
    auto names_ptr = out_buff_ptr + entries.size();

    for(auto const& e : entries) {
        if(e.first.empty()) {
            GKFS_DATA->spdlogger()->warn(
                    "{}() Entry in readdir() empty. If this shows up, something else is very wrong.",
                    __func__);
        }
        *bool_ptr = e.second;
        bool_ptr++;

        const auto name = e.first.c_str();
        ::strcpy(names_ptr, name);
        // number of characters + \0 terminator
        names_ptr += e.first.size() + 1;
    }

    GKFS_DATA->spdlogger()->trace(
            "{}() path '{}' entries '{}' out_size '{}'. Copied data to bulk_buffer. NEXT bulk_transfer",
            __func__, in.path, entries.size(), out_size);

    ret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr, in.bulk_handle, 0,
                              bulk_handle, 0, out_size);
    if(ret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to push '{}' dirents on path '{}' to client with bulk size '{}' and out_size '{}'",
                __func__, entries.size(), in.path, bulk_size, out_size);
        out.err = EBUSY;
        return gkfs::rpc::cleanup_respond(&handle, &in, &out, &bulk_handle);
    }

    out.dirents_size = entries.size();
    out.err = 0;
    GKFS_DATA->spdlogger()->debug(
            "{}() Sending output response err '{}' dirents_size '{}'. DONE",
            __func__, out.err, out.dirents_size);
    if(GKFS_DATA->enable_stats()) {
        GKFS_DATA->stats()->add_value_iops(
                gkfs::utils::Stats::IopsOp::iops_dirent);
    }
    return gkfs::rpc::cleanup_respond(&handle, &in, &out, &bulk_handle);
}

/* Sends the name-size-ctime of a specific directory
 * Used to accelerate find
 * It mimics get_dirents, but uses a tuple
 */

/**
 * @brief Serves a request to return all file system objects in a directory
 * including their size and create timestamp.
 * @internal
 * This is an extension to the above rpc_srv_get_dirents. However, this handler
 * is an optimization which needs to be refactored and merged with with
 * rpc_srv_get_dirents due to redundant code (TODO).
 *
 * Note, the bulk buffer size is decided by the client statically although it
 * doesn't know if it the space is sufficient to accommodate all entries. This
 * is planned to be fixed in the future (TODO).
 *
 * All exceptions must be caught here and dealt with accordingly. Any errors are
 * placed in the response.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_get_dirents_extended(hg_handle_t handle) {
    rpc_get_dirents_in_t in{};
    rpc_get_dirents_out_t out{};
    out.err = EIO;
    out.dirents_size = 0;
    hg_bulk_t bulk_handle = nullptr;

    // Get input parmeters
    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error(
                "{}() Could not get RPC input data with err '{}'", __func__,
                ret);
        out.err = EBUSY;
        return gkfs::rpc::cleanup_respond(&handle, &in, &out);
    }

    // Retrieve size of source buffer
    auto hgi = margo_get_info(handle);
    auto mid = margo_hg_info_get_instance(hgi);
    auto bulk_size = margo_bulk_get_size(in.bulk_handle);
    GKFS_DATA->spdlogger()->debug("{}() Got RPC: path '{}' bulk_size '{}' ",
                                  __func__, in.path, bulk_size);

    // Get directory entries from local DB
    vector<tuple<string, bool, size_t, time_t>> entries{};
    try {
        entries = gkfs::metadata::get_dirents_extended(in.path);
    } catch(const ::exception& e) {
        GKFS_DATA->spdlogger()->error("{}() Error during get_dirents(): '{}'",
                                      __func__, e.what());
        return gkfs::rpc::cleanup_respond(&handle, &in, &out);
    }

    GKFS_DATA->spdlogger()->trace(
            "{}() path '{}' Read database with '{}' entries", __func__, in.path,
            entries.size());

    if(entries.empty()) {
        out.err = 0;
        return gkfs::rpc::cleanup_respond(&handle, &in, &out);
    }

    // Calculate total output size
    // TODO OPTIMIZATION: this can be calculated inside db_get_dirents
    size_t tot_names_size = 0;
    for(auto const& e : entries) {
        tot_names_size += (get<0>(e)).size();
    }

    // tot_names_size (# characters in entry) + # entries * (bool size + char
    // size for \0 character)
    size_t out_size =
            tot_names_size + entries.size() * (sizeof(bool) + sizeof(char) +
                                               sizeof(size_t) + sizeof(time_t));
    if(bulk_size < out_size) {
        // Source buffer is smaller than total output size
        GKFS_DATA->spdlogger()->error(
                "{}() Entries do not fit source buffer. bulk_size '{}' < out_size '{}' must be satisfied!",
                __func__, bulk_size, out_size);
        out.err = ENOBUFS;
        return gkfs::rpc::cleanup_respond(&handle, &in, &out);
    }

    void* bulk_buf; // buffer for bulk transfer
    // create bulk handle and allocated memory for buffer with out_size
    // information
    ret = margo_bulk_create(mid, 1, nullptr, &out_size, HG_BULK_READ_ONLY,
                            &bulk_handle);
    if(ret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error("{}() Failed to create bulk handle",
                                      __func__);
        return gkfs::rpc::cleanup_respond(&handle, &in, &out, &bulk_handle);
    }
    // access the internally allocated memory buffer and put it into bulk_buf
    uint32_t actual_count; // number of segments. we use one here because we
                           // push the whole buffer at once
    ret = margo_bulk_access(bulk_handle, 0, out_size, HG_BULK_READ_ONLY, 1,
                            &bulk_buf, &out_size, &actual_count);
    if(ret != HG_SUCCESS || actual_count != 1) {
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to access allocated buffer from bulk handle",
                __func__);
        return gkfs::rpc::cleanup_respond(&handle, &in, &out, &bulk_handle);
    }

    GKFS_DATA->spdlogger()->trace(
            "{}() path '{}' entries '{}' out_size '{}'. Set up local read only bulk handle and allocated buffer with size '{}'",
            __func__, in.path, entries.size(), out_size, out_size);

    // Serialize output data on local buffer
    // The parenthesis are extremely important, if not the + will be size_t or
    // time_t size and not char
    auto out_buff_ptr = static_cast<char*>(bulk_buf);
    auto bool_ptr = reinterpret_cast<bool*>(out_buff_ptr);
    auto size_ptr = reinterpret_cast<size_t*>((out_buff_ptr) +
                                              (entries.size() * sizeof(bool)));
    auto ctime_ptr = reinterpret_cast<time_t*>(
            (out_buff_ptr) +
            (entries.size() * (sizeof(bool) + sizeof(size_t))));
    auto names_ptr =
            out_buff_ptr +
            (entries.size() * (sizeof(bool) + sizeof(size_t) + sizeof(time_t)));

    for(auto const& e : entries) {
        if((get<0>(e)).empty()) {
            GKFS_DATA->spdlogger()->warn(
                    "{}() Entry in readdir() empty. If this shows up, something else is very wrong.",
                    __func__);
        }
        *bool_ptr = (get<1>(e));
        bool_ptr++;

        *size_ptr = (get<2>(e));
        size_ptr++;

        *ctime_ptr = (get<3>(e));
        ctime_ptr++;

        const auto name = (get<0>(e)).c_str();
        ::strcpy(names_ptr, name);
        // number of characters + \0 terminator
        names_ptr += ((get<0>(e)).size() + 1);
    }

    GKFS_DATA->spdlogger()->trace(
            "{}() path '{}' entries '{}' out_size '{}'. Copied data to bulk_buffer. NEXT bulk_transfer",
            __func__, in.path, entries.size(), out_size);
    ret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr, in.bulk_handle, 0,
                              bulk_handle, 0, out_size);
    if(ret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to push '{}' dirents on path '{}' to client with bulk size '{}' and out_size '{}'",
                __func__, entries.size(), in.path, bulk_size, out_size);
        out.err = EBUSY;
        return gkfs::rpc::cleanup_respond(&handle, &in, &out, &bulk_handle);
    }

    out.dirents_size = entries.size();
    out.err = 0;
    GKFS_DATA->spdlogger()->debug(
            "{}() Sending output response err '{}' dirents_size '{}'. DONE",
            __func__, out.err, out.dirents_size);
    return gkfs::rpc::cleanup_respond(&handle, &in, &out, &bulk_handle);
}

#if defined(HAS_SYMLINKS) || defined(HAS_RENAME)
/**
 * @brief Serves a request create a symbolic link and supports rename
 * @internal
 * The state of this function is unclear and requires a complete refactor.
 *
 * All exceptions must be caught here and dealt with accordingly. Any errors are
 * placed in the response.
 * @endinteral
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_mk_symlink(hg_handle_t handle) {
    rpc_mk_symlink_in_t in{};
    rpc_err_out_t out{};

    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error(
                "{}() Failed to retrieve input from handle", __func__);
    }
    GKFS_DATA->spdlogger()->debug(
            "{}() Got RPC with path '{}' and target path '{}'", __func__,
            in.path, in.target_path);
    // do update
    try {
        gkfs::metadata::Metadata md = gkfs::metadata::get(in.path);
#ifdef HAS_RENAME
        if(md.blocks() == -1) {
            // We need to fill the rename path as this is an inverse path
            // old -> new
            md.rename_path(in.target_path);
        } else {
#endif // HAS_RENAME
            md.target_path(in.target_path);
#ifdef HAS_RENAME
        }
#endif // HAS_RENAME
        GKFS_DATA->spdlogger()->debug(
                "{}() Updating path '{}' with metadata '{}'", __func__, in.path,
                md.serialize());
        gkfs::metadata::update(in.path, md);
        out.err = 0;
    } catch(const std::exception& e) {
        // TODO handle NotFoundException
        GKFS_DATA->spdlogger()->error("{}() Failed to update entry", __func__);
        out.err = 1;
    }

    GKFS_DATA->spdlogger()->debug("{}() Sending output err '{}'", __func__,
                                  out.err);
    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        GKFS_DATA->spdlogger()->error("{}() Failed to respond", __func__);
    }

    // Destroy handle when finished
    margo_free_input(handle, &in);
    margo_destroy(handle);
    return HG_SUCCESS;
}

#endif // HAS_SYMLINKS || HAS_RENAME

} // namespace

DEFINE_MARGO_RPC_HANDLER(rpc_srv_create)

DEFINE_MARGO_RPC_HANDLER(rpc_srv_stat)

DEFINE_MARGO_RPC_HANDLER(rpc_srv_decr_size)

DEFINE_MARGO_RPC_HANDLER(rpc_srv_remove_metadata)

DEFINE_MARGO_RPC_HANDLER(rpc_srv_remove_data)

DEFINE_MARGO_RPC_HANDLER(rpc_srv_update_metadentry)

DEFINE_MARGO_RPC_HANDLER(rpc_srv_update_metadentry_size)

DEFINE_MARGO_RPC_HANDLER(rpc_srv_get_metadentry_size)

DEFINE_MARGO_RPC_HANDLER(rpc_srv_get_dirents)

DEFINE_MARGO_RPC_HANDLER(rpc_srv_get_dirents_extended)
#ifdef HAS_SYMLINKS

DEFINE_MARGO_RPC_HANDLER(rpc_srv_mk_symlink)

#endif
