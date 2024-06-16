/**
 * Copyright (c) 2013-2021 UChicago Argonne, LLC and The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "mercury_test.h"

#include <unistd.h>

/****************/
/* Local Macros */
/****************/

/************************************/
/* Local Type and Struct Definition */
/************************************/

struct forward_cb_args {
    hg_request_t *request;
};

/********************/
/* Local Prototypes */
/********************/

static hg_return_t
hg_test_rpc_forward_killed_cb(const struct hg_cb_info *callback_info);

static hg_return_t
hg_test_killed_rpc(hg_context_t *context, hg_request_class_t *request_class,
    hg_addr_t addr, hg_id_t rpc_id, hg_cb_t callback);

/*******************/
/* Local Variables */
/*******************/

extern hg_id_t hg_test_killed_rpc_id_g;

/*---------------------------------------------------------------------------*/
static hg_return_t
hg_test_rpc_forward_killed_cb(const struct hg_cb_info *callback_info)
{
    hg_request_t *request = (hg_request_t *) callback_info->arg;
    hg_return_t ret = HG_SUCCESS;

    if (callback_info->ret == HG_CANCELED)
        HG_TEST_LOG_DEBUG("HG_Forward() was successfully canceled");
    else
        HG_TEST_CHECK_ERROR_NORET(callback_info->ret != HG_SUCCESS, done,
            "Error in HG callback (%s)",
            HG_Error_to_string(callback_info->ret));

done:
    hg_request_complete(request);
    return ret;
}

/*---------------------------------------------------------------------------*/
static hg_return_t
hg_test_killed_rpc(hg_context_t *context, hg_request_class_t *request_class,
    hg_addr_t addr, hg_id_t rpc_id, hg_cb_t callback)
{
    hg_request_t *request = NULL;
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_return_t ret = HG_SUCCESS, cleanup_ret;

    request = hg_request_create(request_class);

    /* Create RPC request */
    ret = HG_Create(context, addr, rpc_id, &handle);
    HG_TEST_CHECK_HG_ERROR(
        done, ret, "HG_Create() failed (%s)", HG_Error_to_string(ret));

    /* Forward call to remote addr and get a new request */
    HG_TEST_LOG_DEBUG("Forwarding RPC, op id: %" PRIu64 "...", rpc_id);
    ret = HG_Forward(handle, callback, request, NULL);
    HG_TEST_CHECK_HG_ERROR(
        done, ret, "HG_Forward() failed (%s)", HG_Error_to_string(ret));

    /* Cancel request before making progress, this ensures that the RPC has not
     * completed yet. */
    ret = HG_Cancel(handle);
    HG_TEST_CHECK_HG_ERROR(
        done, ret, "HG_Cancel() failed (%s)", HG_Error_to_string(ret));

    hg_request_wait(request, HG_MAX_IDLE_TIME, NULL);

done:
    cleanup_ret = HG_Destroy(handle);
    HG_TEST_CHECK_ERROR_DONE(cleanup_ret != HG_SUCCESS,
        "HG_Destroy() failed (%s)", HG_Error_to_string(cleanup_ret));

    hg_request_destroy(request);

    return ret;
}

/*---------------------------------------------------------------------------*/
int
main(int argc, char *argv[])
{
    struct hg_test_info hg_test_info = {0};
    hg_return_t hg_ret;
    int ret = EXIT_SUCCESS;

    /* Initialize the interface */
    hg_ret = HG_Test_init(argc, argv, &hg_test_info);
    HG_TEST_CHECK_ERROR(
        hg_ret != HG_SUCCESS, done, ret, EXIT_FAILURE, "HG_Test_init() failed");

    /* Cancel RPC test (self cancelation is not supported) */
    if (!hg_test_info.na_test_info.self_send) {
        HG_TEST("interrupted RPC");
        hg_ret = hg_test_killed_rpc(hg_test_info.context,
            hg_test_info.request_class, hg_test_info.target_addr,
            hg_test_killed_rpc_id_g, hg_test_rpc_forward_killed_cb);
        HG_TEST_CHECK_ERROR(hg_ret != HG_SUCCESS, done, ret, EXIT_FAILURE,
            "interrupted RPC test failed");
        HG_PASSED();
    }

    /* Sleep for 1s to let the server exit */
    sleep(1);

    /* After that point, we need to silence errors */
    hg_log_set_level(HG_LOG_LEVEL_NONE);

    if (!hg_test_info.na_test_info.self_send) {
        HG_TEST("attempt second interrupted RPC");
        hg_ret = hg_test_killed_rpc(hg_test_info.context,
            hg_test_info.request_class, hg_test_info.target_addr,
            hg_test_killed_rpc_id_g, hg_test_rpc_forward_killed_cb);
        HG_PASSED();
    }

done:
    if (ret != EXIT_SUCCESS)
        HG_FAILED();

    hg_ret = HG_Test_finalize(&hg_test_info);
    HG_TEST_CHECK_ERROR_DONE(hg_ret != HG_SUCCESS, "HG_Test_finalize() failed");

    return ret;
}
