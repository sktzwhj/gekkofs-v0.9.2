/**
 * Copyright (c) UT-Battelle, LLC. 2014-2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 *
 * Copyright (C) Computer Institute, NUDT, China, 2018. ALL RIGHTS RESERVED.
 */

#ifndef UCT_GLEX_IFACE_H
#define UCT_GLEX_IFACE_H

#include <ucs/datastruct/arbiter.h>
#include <ucs/datastruct/ptr_array.h>
#include <ucs/datastruct/mpool.h>
#include <uct/base/uct_iface.h>
#include "glex_md.h"
#include "glex_def.h"

#define UCT_GLEX_TL_NAME "glex"

typedef struct uct_glex_iface_config {
    uct_iface_config_t          super;
    uct_iface_mpool_config_t    mpool;
} uct_glex_iface_config_t;

typedef struct uct_glex_iface {
    uct_base_iface_t    super;
    uct_glex_md_t       *glex_md;	//XXX, TODO, md is included in uct_base_iface
    glex_ep_handle_t    glex_ep;
    glex_ep_addr_t      address;
    ucs_queue_head_t    vc_tp_queue;    /* to be connected/released vc */
    ucs_arbiter_t       arbiter;
    ucs_mpool_t         flush_pool;
    ucs_ptr_array_t     send_stat;	/* status of SRQ/ERQ/ZC send request */
    unsigned int        outstanding;
    uint32_t            key;

    khash_t(uct_glex_vc)        vc_hash;
    uint32_t                    hash_key;

    /* MP reorder list */
    ucs_mpool_t         mp_info;
    uct_glex_mp_info_t  *credit_mp_info_list;
    uint32_t            mp_cnt;
    uint32_t            pending_mp_cnt; /* probed but unprocessed MP */

    struct {
        ucs_mpool_t             free_srq_desc;
        ucs_ptr_array_t         recv_info;	/* status of recv SRQ */
        unsigned int            srq_idle;
        unsigned int            srq_limit;
        unsigned int            srq_send_used;
        unsigned int            srq_recv_used;
        ucs_queue_head_t        mp_vc_queue;     /* scan pending MP */
        ucs_queue_head_t        recv_vc_queue;   /* scan pending recv srq */
        ucs_queue_head_t        credit_vc_queue; /* send credit MP */
        ucs_queue_head_t        send_queue;      /* progress send srq in RDMA PUT mode */
        /* dynamic flow control */
        unsigned int            credit_inc;
        unsigned int            credit_max;
        int                     credit_pool;
        /* RDMA mode */
        int                     rdma_put_mode;
    } sr;

    struct {
        ucs_mpool_t             erq_pool;
        ucs_queue_head_t        recv_vc_queue;  /* scan pending recv srq */
        unsigned int            channels;
        uint32_t                q_min_credits;
        uint32_t                q_capacity;
        uint32_t                q_thresh;
        uint32_t                q_unit_shift;
        uint32_t                q_unit_mask;
    } er;

    struct {
        ucs_mpool_t             free_buf_desc;  /* bcopy buffer */
        ucs_mpool_t             free_zc_desc;   /* zc request */
        unsigned int            req_idle;       /* number of idle bcopy buffers */
        unsigned int            zc_req_idle;    /* number of idle zc requests */
    } rma;
} uct_glex_iface_t;

#endif
