/**
 * Copyright (c) 2013-2021 UChicago Argonne, LLC and The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "na_test.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NA_TEST_BULK_SIZE    1024 * 1024
#define NA_TEST_BULK_TAG     102
#define NA_TEST_BULK_ACK_TAG 103

static int test_done_g = 0;

/* Test parameters */
struct na_test_params {
    na_class_t *na_class;
    na_context_t *context;
    na_addr_t source_addr;
    char *send_buf;
    char *recv_buf;
    void *send_buf_plugin_data;
    void *recv_buf_plugin_data;
    int *bulk_buf;
    na_size_t send_buf_len;
    na_size_t recv_buf_len;
    na_size_t bulk_size;
    na_mem_handle_t local_mem_handle;
    na_mem_handle_t remote_mem_handle;
};

/* NA test routines */
static int
test_send_respond(struct na_test_params *params, na_tag_t send_tag);
#ifdef NA_HAS_CCI
static int
test_bulk_prepare(struct na_test_params *params);
#endif

static int
msg_unexpected_recv_cb(const struct na_cb_info *callback_info)
{
    struct na_test_params *params =
        (struct na_test_params *) callback_info->arg;
    na_tag_t recv_tag;
    na_return_t ret = NA_SUCCESS;

    if (callback_info->ret == NA_CANCELED) {
        /* Try again */
        printf("NA_Msg_recv_unexpected() was successfully canceled\n");
        ret = NA_Msg_recv_unexpected(params->na_class, params->context,
            msg_unexpected_recv_cb, params, params->recv_buf,
            params->recv_buf_len, params->recv_buf_plugin_data,
            NA_OP_ID_IGNORE);
        if (ret != NA_SUCCESS) {
            fprintf(stderr, "Could not post recv of unexpected message\n");
        }
        return ret;
    } else {
        printf("NA_Msg_recv_unexpected() was not canceled\n");
    }

    if (callback_info->ret != NA_SUCCESS) {
        return ret;
    }

    printf("Received msg (%s) from client\n", params->recv_buf);

    params->source_addr = callback_info->info.recv_unexpected.source;
    recv_tag = callback_info->info.recv_unexpected.tag;

    test_send_respond(params, recv_tag + 1);
#ifdef NA_HAS_CCI
    if (strcmp(NA_Get_class_name(params->na_class), "cci") == 0)
        test_bulk_prepare(params);
    else
#endif
        test_done_g = 1;

    return ret;
}

#ifdef NA_HAS_CCI
static int
msg_expected_send_final_cb(const struct na_cb_info *callback_info)
{
    na_return_t ret = NA_SUCCESS;

    if (callback_info->ret != NA_SUCCESS) {
        return ret;
    }

    test_done_g = 1;

    return ret;
}

static int
bulk_put_cb(const struct na_cb_info *callback_info)
{
    struct na_test_params *params =
        (struct na_test_params *) callback_info->arg;
    na_tag_t ack_tag = NA_TEST_BULK_ACK_TAG;
    na_return_t ret = NA_SUCCESS;

    if (callback_info->ret == NA_CANCELED) {
        /* Try again */
        printf("NA_Put() was successfully canceled\n");
        ret = NA_Put(params->na_class, params->context, bulk_put_cb, params,
            params->local_mem_handle, 0, params->remote_mem_handle, 0,
            params->bulk_size * sizeof(int), params->source_addr, 0,
            NA_OP_ID_IGNORE);
        if (ret != NA_SUCCESS) {
            fprintf(stderr, "Could not start put\n");
        }
        return ret;
    } else {
        printf("NA_Put() was not canceled\n");
    }

    if (callback_info->ret != NA_SUCCESS) {
        return ret;
    }

    /* Send completion ack */
    printf("Sending end of transfer ack...\n");
    ret = NA_Msg_send_expected(params->na_class, params->context,
        msg_expected_send_final_cb, NULL, params->send_buf,
        params->send_buf_len, params->send_buf_plugin_data, params->source_addr,
        0, ack_tag, NA_OP_ID_IGNORE);
    if (ret != NA_SUCCESS) {
        fprintf(stderr, "Could not start send of acknowledgment\n");
        return ret;
    }

    /* Free memory and addresses */
    ret = NA_Mem_deregister(params->na_class, params->local_mem_handle);
    if (ret != NA_SUCCESS) {
        fprintf(stderr, "Could not unregister memory\n");
        return ret;
    }
    ret = NA_Mem_handle_free(params->na_class, params->local_mem_handle);
    if (ret != NA_SUCCESS) {
        fprintf(stderr, "Could not free memory handle\n");
        return ret;
    }
    ret = NA_Mem_handle_free(params->na_class, params->remote_mem_handle);
    if (ret != NA_SUCCESS) {
        fprintf(stderr, "Could not free memory handle\n");
        return ret;
    }

    return ret;
}

static int
bulk_get_cb(const struct na_cb_info *callback_info)
{
    struct na_test_params *params =
        (struct na_test_params *) callback_info->arg;
    na_return_t ret = NA_SUCCESS;
    unsigned int i;
    na_bool_t error = 0;
    na_op_id_t op_id;

    if (callback_info->ret == NA_CANCELED) {
        /* Try again */
        printf("NA_Get() was successfully canceled\n");
        ret = NA_Get(params->na_class, params->context, bulk_get_cb, params,
            params->local_mem_handle, 0, params->remote_mem_handle, 0,
            params->bulk_size * sizeof(int), params->source_addr, 0,
            NA_OP_ID_IGNORE);
        if (ret != NA_SUCCESS) {
            fprintf(stderr, "Could not start get\n");
        }
        return ret;
    } else {
        printf("NA_Get() was not canceled\n");
    }

    if (callback_info->ret != NA_SUCCESS) {
        return ret;
    }

    /* Check bulk buf */
    for (i = 0; i < params->bulk_size; i++) {
        if ((na_size_t) params->bulk_buf[i] != i) {
            printf("Error detected in bulk transfer, bulk_buf[%u] = %d,\t"
                   " was expecting %u!\n",
                i, params->bulk_buf[i], i);
            error = 1;
            break;
        }
    }
    if (!error)
        printf("Successfully transfered %zu bytes!\n",
            (size_t) params->bulk_size * sizeof(int));

    /* Reset bulk_buf */
    printf("Resetting buffer\n");
    memset(params->bulk_buf, 0, params->bulk_size * sizeof(int));

    /* Now do a put */
    printf("Putting %d bytes to remote...\n",
        (int) (params->bulk_size * sizeof(int)));

    ret = NA_Put(params->na_class, params->context, bulk_put_cb, params,
        params->local_mem_handle, 0, params->remote_mem_handle, 0,
        params->bulk_size * sizeof(int), params->source_addr, 0, &op_id);
    if (ret != NA_SUCCESS) {
        fprintf(stderr, "Could not start put\n");
    }

    ret = NA_Cancel(params->na_class, params->context, op_id);
    if (ret != NA_SUCCESS) {
        fprintf(stderr, "Could not cancel put operation\n");
    }

    return ret;
}

static int
mem_handle_expected_recv_cb(const struct na_cb_info *callback_info)
{
    struct na_test_params *params =
        (struct na_test_params *) callback_info->arg;
    na_op_id_t op_id;
    na_return_t ret = NA_SUCCESS;

    if (callback_info->ret != NA_SUCCESS) {
        return ret;
    }

    /* Deserialize memory handle */
    printf("Deserializing remote memory handle...\n");
    ret = NA_Mem_handle_deserialize(params->na_class,
        &params->remote_mem_handle, params->recv_buf, params->recv_buf_len);
    if (ret != NA_SUCCESS) {
        fprintf(stderr, "Could not deserialize memory handle\n");
        return ret;
    }

    /* Do a get */
    printf("Getting %d bytes from remote...\n",
        (int) (params->bulk_size * sizeof(int)));

    ret = NA_Get(params->na_class, params->context, bulk_get_cb, params,
        params->local_mem_handle, 0, params->remote_mem_handle, 0,
        params->bulk_size * sizeof(int), params->source_addr, 0, &op_id);
    if (ret != NA_SUCCESS) {
        fprintf(stderr, "Could not start get\n");
        return ret;
    }

    ret = NA_Cancel(params->na_class, params->context, op_id);
    if (ret != NA_SUCCESS) {
        fprintf(stderr, "Could not cancel get operation\n");
    }

    return ret;
}
#endif

static int
test_send_respond(struct na_test_params *params, na_tag_t send_tag)
{
    na_return_t na_ret;

    /* Respond back */
    sprintf(params->send_buf, "Hello Client!");

    na_ret = NA_Msg_send_expected(params->na_class, params->context, NULL, NULL,
        params->send_buf, params->send_buf_len, params->send_buf_plugin_data,
        params->source_addr, 0, send_tag, NA_OP_ID_IGNORE);
    if (na_ret != NA_SUCCESS) {
        fprintf(stderr, "Could not start send of message\n");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

#ifdef NA_HAS_CCI
static int
test_bulk_prepare(struct na_test_params *params)
{
    na_tag_t bulk_tag = NA_TEST_BULK_TAG;
    na_return_t na_ret;

    /* Register memory */
    printf("Registering local memory...\n");
    na_ret = NA_Mem_handle_create(params->na_class, params->bulk_buf,
        sizeof(int) * params->bulk_size, NA_MEM_READWRITE,
        &params->local_mem_handle);
    if (na_ret != NA_SUCCESS) {
        fprintf(stderr, "Could not create bulk handle\n");
        return EXIT_FAILURE;
    }

    na_ret = NA_Mem_register(params->na_class, params->local_mem_handle);
    if (na_ret != NA_SUCCESS) {
        fprintf(stderr, "Could not create bulk handle\n");
        return EXIT_FAILURE;
    }

    /* Recv memory handle */
    printf("Receiving remote memory handle...\n");
    na_ret = NA_Msg_recv_expected(params->na_class, params->context,
        mem_handle_expected_recv_cb, params, params->recv_buf,
        params->recv_buf_len, params->recv_buf_plugin_data, params->source_addr,
        0, bulk_tag, NA_OP_ID_IGNORE);
    if (na_ret != NA_SUCCESS) {
        fprintf(stderr, "Could not start recv of memory handle\n");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
#endif

int
main(int argc, char *argv[])
{
    struct na_test_info na_test_info = {0};
    int peer;
    struct na_test_params params;
    na_return_t na_ret;

    /* Initialize the interface */
    na_test_info.listen = NA_TRUE;
    NA_Test_init(argc, argv, &na_test_info);

    params.na_class = na_test_info.na_class;
    params.context = NA_Context_create(params.na_class);

    /* Allocate send/recv/bulk bufs */
    params.send_buf_len = NA_Msg_get_max_unexpected_size(params.na_class);
    params.recv_buf_len = params.send_buf_len;
    params.send_buf = (char *) NA_Msg_buf_alloc(
        params.na_class, params.send_buf_len, &params.send_buf_plugin_data);
    params.recv_buf = (char *) NA_Msg_buf_alloc(
        params.na_class, params.recv_buf_len, &params.recv_buf_plugin_data);

    /* Prepare bulk_buf */
    params.bulk_size = NA_TEST_BULK_SIZE;
    params.bulk_buf = (int *) malloc(params.bulk_size * sizeof(int));

    for (peer = 0; peer < na_test_info.max_number_of_peers; peer++) {
        unsigned int i;
        na_op_id_t op_id = NA_OP_ID_NULL;

        /* Reset to 0 */
        for (i = 0; i < params.bulk_size; i++) {
            params.bulk_buf[i] = 0;
        }

        /* Recv a message from a client */
        na_ret = NA_Msg_recv_unexpected(params.na_class, params.context,
            msg_unexpected_recv_cb, &params, params.recv_buf,
            params.recv_buf_len, params.recv_buf_plugin_data, &op_id);
        if (na_ret != NA_SUCCESS) {
            fprintf(stderr, "Could not post recv of unexpected message\n");
            return EXIT_FAILURE;
        }

        na_ret = NA_Cancel(params.na_class, params.context, op_id);
        if (na_ret != NA_SUCCESS) {
            fprintf(stderr, "Could not cancel recv of unexpected message\n");
            return EXIT_FAILURE;
        }

        while (!test_done_g) {
            na_return_t trigger_ret;
            unsigned int actual_count = 0;

            do {
                trigger_ret =
                    NA_Trigger(params.context, 0, 1, NULL, &actual_count);
            } while ((trigger_ret == NA_SUCCESS) && actual_count);

            if (test_done_g)
                break;

            na_ret =
                NA_Progress(params.na_class, params.context, NA_MAX_IDLE_TIME);
            if (na_ret != NA_SUCCESS && na_ret != NA_TIMEOUT) {
                return EXIT_SUCCESS;
            }
        }

        na_ret = NA_Addr_free(params.na_class, params.source_addr);
        if (na_ret != NA_SUCCESS) {
            fprintf(stderr, "Could not free addr\n");
            return EXIT_FAILURE;
        }
        params.source_addr = NA_ADDR_NULL;
        test_done_g = 0;
    }

    printf("Finalizing...\n");

    free(params.bulk_buf);
    NA_Msg_buf_free(
        params.na_class, params.recv_buf, params.recv_buf_plugin_data);
    NA_Msg_buf_free(
        params.na_class, params.send_buf, params.send_buf_plugin_data);

    NA_Context_destroy(params.na_class, params.context);

    NA_Test_finalize(&na_test_info);

    return EXIT_SUCCESS;
}
