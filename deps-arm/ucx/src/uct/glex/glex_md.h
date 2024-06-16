/**
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 * Copyright (c) UT-Battelle, LLC. 2014-2015. ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 *
 * Copyright (C) Computer Institute, NUDT, China, 2018. ALL RIGHTS RESERVED.
 */

#ifndef UCT_GLEX_MD_H_
#define UCT_GLEX_MD_H_

#include <ucs/config/types.h>
#include <ucs/debug/memtrack_int.h>
#include <ucs/type/status.h>
#include <ucs/memory/rcache.h>
#include <uct/base/uct_md.h>
#include <uct/base/uct_iface.h>
#include <uct/api/uct.h>
#include <ucm/api/ucm.h>
#include <glex.h>

extern uct_component_t uct_glex_component;

typedef struct uct_glex_channel_config {
    int                 ep_type;
    int                 ep_hc_mpq;
    int                 ep_hc_eq;
    int                 ep_hc_mr;
    unsigned int        ep_dq_capacity;
    unsigned int        ep_mpq_capacity;
    unsigned int        ep_eq_capacity;
    unsigned int        ep_mpq_pool_units;
    unsigned int        srq_capacity;
    size_t              srq_unit_size;
    unsigned int        er_max_channels;
    size_t              erq_size;       /* one half for send, another half for recv */
    size_t              erq_unit_size;
    size_t              rma_buf_size;
    unsigned int        rma_req_capacity;
    unsigned int        zc_req_capacity;
    unsigned int        sr_credit_start;
    unsigned int        sr_credit_inc;
    unsigned int        sr_credit_max;
    int                 sr_rdma_put;
    unsigned int        num_procs;
} uct_glex_channel_config_t;

/**
 * @brief GLEX MD descriptor
 */
typedef struct uct_glex_md {
    struct uct_md               super;    /**< Domain info */
    glex_device_handle_t        dev;
    struct glex_device_attr     dev_attr;
    glex_ep_handle_t            ep;       /**< GLEX endpoint */
    struct glex_ep_attr         ep_attr;
    ucs_rcache_t                *rcache;  /**< Registration cache (can be NULL) */
    ucs_linear_func_t           reg_cost; /**< Memory registration cost */
    uint32_t                    dev_id;
    struct {
        size_t                  srq_unit_size;
        unsigned int            srq_capacity;
        size_t                  erq_size;
        unsigned int            erq_unit_size;
        unsigned int            er_max_channels;
        unsigned int            zc_req_capacity; /* XXX, TODO, why set this limit? */
        size_t                  rma_buf_size;
        unsigned int            rma_req_capacity;
        unsigned int            sr_credit_start;
        unsigned int            sr_credit_inc;
        unsigned int            sr_credit_max;
        int                     sr_credit_pool;
        int                     sr_rdma_put;
    } config;
} uct_glex_md_t;

/**
 * @brief GLEX packed and remote key
 */
typedef struct uct_glex_key {
    glex_mem_handle_t   mh;      /**< GLEX handle of registered memory */
    uintptr_t           address; /**< Base addr of the memory */
} uct_glex_key_t;

/**
 * GLEX memory domain configuration.
 */
typedef struct uct_glex_md_config {
    uct_md_config_t             super;
    ucs_ternary_auto_value_t    rcache_enable;
    uct_md_rcache_config_t      rcache;
    uct_glex_channel_config_t   channel;
} uct_glex_md_config_t;

/**
 * GLEX memory region in the registration cache.
 */
typedef struct uct_glex_rcache_region {
    ucs_rcache_region_t super;
    uct_glex_key_t      key;      /**< exposed to the user as the memh */
} uct_glex_rcache_region_t;

#endif
