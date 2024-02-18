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
 * @brief Provide helper functions for Margo's RPC interfaces reduce code
 * verbosity of the RPC handler functions.
 * @internal
 * Note, this is a temporary solution and is planned to be refactored.
 * @endinternal
 */

#ifndef GEKKOFS_DAEMON_RPC_UTIL_HPP
#define GEKKOFS_DAEMON_RPC_UTIL_HPP

extern "C" {
#include <mercury_types.h>
#include <mercury_proc_string.h>
#include <margo.h>
}

#include <string>

namespace gkfs::rpc {

/**
 * @brief Frees all given RPC resources allocated by Margo.
 * @tparam InputType Any RPC input struct from client requests
 * @tparam OutputType Any RPC output struct for client response
 * @param handle Pointer to Mercury RPC handle
 * @param input Pointer to input struct
 * @param output Pointer to output struct
 * @param bulk_handle Pointer to Mercury bulk handle
 * @return Mercury error code. HG_SUCCESS on success.
 */
template <typename InputType, typename OutputType>
inline hg_return_t
cleanup(hg_handle_t* handle, InputType* input, OutputType* output,
        hg_bulk_t* bulk_handle) {
    auto ret = HG_SUCCESS;
    if(bulk_handle) {
        ret = margo_bulk_free(*bulk_handle);
        if(ret != HG_SUCCESS)
            return ret;
    }
    if(input && handle) {
        ret = margo_free_input(*handle, input);
        if(ret != HG_SUCCESS)
            return ret;
    }
    if(output && handle) {
        ret = margo_free_output(*handle, output);
        if(ret != HG_SUCCESS)
            return ret;
    }
    if(handle) {
        ret = margo_destroy(*handle);
        if(ret != HG_SUCCESS)
            return ret;
    }
    return ret;
}

/**
 * @brief Responds to a client request.
 * @internal
 * Note, Mercury frees the output struct itself after it responded to the
 * client. Attempting to explicitly free the output struct can cause segfaults
 * because the response is non-blocking and we could free the resources before
 * Mercury has responded.
 * @endinternal
 *
 * @tparam OutputType Any RPC output struct for client response
 * @param handle Pointer to Mercury RPC handle
 * @param output Pointer to output struct
 * @return Mercury error code. HG_SUCCESS on success.
 */
template <typename OutputType>
inline hg_return_t
respond(hg_handle_t* handle, OutputType* output) {
    auto ret = HG_SUCCESS;
    if(output && handle) {
        ret = margo_respond(*handle, output);
        if(ret != HG_SUCCESS)
            return ret;
    }
    return ret;
}
/**
 * @brief Combines responding to the client and cleaning up all RPC resources
 * after.
 * @tparam InputType Any RPC input struct from client requests
 * @tparam OutputType Any RPC output struct for client response
 * @param handle Pointer to Mercury RPC handle
 * @param input Pointer to input struct
 * @param output Pointer to output struct
 * @param bulk_handle Pointer to Mercury bulk handle
 * @return Mercury error code. HG_SUCCESS on success.
 */
template <typename InputType, typename OutputType>
inline hg_return_t
cleanup_respond(hg_handle_t* handle, InputType* input, OutputType* output,
                hg_bulk_t* bulk_handle) {
    auto ret = respond(handle, output);
    if(ret != HG_SUCCESS)
        return ret;
    return cleanup(handle, input, static_cast<OutputType*>(nullptr),
                   bulk_handle);
}
/**
 * @brief Combines responding to the client and cleaning up all RPC resources
 * after.
 * @tparam InputType Any RPC input struct from client requests
 * @tparam OutputType Any RPC output struct for client response
 * @param handle Pointer to Mercury RPC handle
 * @param input Pointer to input struct
 * @param output Pointer to output struct
 * @return Mercury error code. HG_SUCCESS on success.
 */
template <typename InputType, typename OutputType>
inline hg_return_t
cleanup_respond(hg_handle_t* handle, InputType* input, OutputType* output) {
    return cleanup_respond(handle, input, output, nullptr);
}
/**
 * @brief Combines responding to the client and cleaning up all RPC resources
 * after.
 * @tparam OutputType Any RPC output struct for client response
 * @param handle Pointer to Mercury RPC handle
 * @param output Pointer to output struct
 * @return Mercury error code. HG_SUCCESS on success.
 */
template <typename OutputType>
inline hg_return_t
cleanup_respond(hg_handle_t* handle, OutputType* output) {
    auto ret = respond(handle, output);
    if(ret != HG_SUCCESS)
        return ret;
    if(handle) {
        ret = margo_destroy(*handle);
        if(ret != HG_SUCCESS)
            return ret;
    }
    return ret;
}

} // namespace gkfs::rpc


#endif // GEKKOFS_DAEMON_RPC_UTIL_HPP
