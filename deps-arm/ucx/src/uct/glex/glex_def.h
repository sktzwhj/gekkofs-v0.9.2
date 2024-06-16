/**
 * Copyright (C) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 *
 * Copyright (C) Computer Institute, NUDT, China, 2018-2020. ALL RIGHTS RESERVED.
 */

#ifndef UCT_GLEX_DEF_H
#define UCT_GLEX_DEF_H

#include <ucs/datastruct/queue_types.h>
#include <ucs/datastruct/list.h>
#include <ucs/datastruct/khash.h>
#include <glex.h>

#define UCT_GLEX_MD_PREFIX              "glex"
#define UCT_GLEX_NI_PREFIX              "gni"
#define UCT_GLEX_HASH_SIZE              256
#define UCT_GLEX_INIT_FLUSH             1
#define UCT_GLEX_INIT_FLUSH_REQ         2

#define UCT_GLEX_MP_DATA_LEN            112
#define UCT_GLEX_IMM_RDMA_DATA_LEN      96

/* SRQ default configuration */
#define UCT_GLEX_SRQ_UNIT_SIZE          "16k"
#define UCT_GLEX_SRQ_CAPACITY           "4080"
#define UCT_GLEX_SR_CREDIT_START        "1"
#define UCT_GLEX_SR_CREDIT_INC          "4"
#define UCT_GLEX_SR_CREDIT_MAX          "128"

/* ERQ default configuration */
#define UCT_GLEX_ER_MAX_CHANNELS        "32"
#define UCT_GLEX_ERQ_SIZE               "1m"
#define UCT_GLEX_ERQ_UNIT_SIZE          "256"
#define UCT_GLEX_ERQ_UNIT_MIN_SIZE      128     /* align cache line, and > MP_DATA_LEN */
#define UCT_GLEX_ERQ_UNIT_MAX_SIZE      (64*1024)
#define UCT_GLEX_ERQ_MPOOL_GROW         16

/* ZC default configuration */
#define UCT_GLEX_ZC_REQ_CAPACITY        "2048"

/* RMA default configuration */
#define UCT_GLEX_RMA_BUF_SIZE           "8k"    // XXX, TODO
#define UCT_GLEX_RMA_REQ_CAPACITY       "2040"

/* resource limit */
#define UCT_GLEX_NUM_PROCS_DEFAULT      "256"
#define UCT_GLEX_KEY_DEFAULT            0xaf
#define UCT_GLEX_SRQ_UNIT_MAX_SIZE      (1024*1024)
#define UCT_GLEX_SRQ_MAX_SIZE           (128*1024*1024)
#define UCT_GLEX_ERQ_MAX_SIZE           (4*1024*1024)
#define UCT_GLEX_RMA_BUF_MAX_SIZE       (1024*1024)

/* type of protocol MP */
enum {
    UCT_GLEX_MP_DIRECT = 0,
    UCT_GLEX_MP_SR_REQ,
    UCT_GLEX_MP_CREDIT,
    UCT_GLEX_MP_ER_CONN_REQ,
    UCT_GLEX_MP_ER_CONN_ACK,
    UCT_GLEX_MP_ER_CONN_NACK
};

typedef struct uct_glex_mp_hdr {
    uint8_t     type;
    uint8_t     credit_inc_req;
    uint16_t    recv_credit;
    uint16_t    seq_num;
    uint16_t    am_id;
} UCS_S_PACKED uct_glex_mp_hdr_t;

typedef struct uct_glex_direct_mp {
    uct_glex_mp_hdr_t   hdr;
    /* XXX, TODO, change to void *data, dynamic allocate direct_mp in iface_open */
    char                data[UCT_GLEX_MP_DATA_LEN-sizeof(uct_glex_mp_hdr_t)];
} UCS_S_PACKED uct_glex_direct_mp_t;

typedef struct uct_glex_sr_req_mp {
    uct_glex_mp_hdr_t   hdr;
    glex_mem_handle_t   mh;
    uint32_t            off;
    uint32_t            srq_idx;
    uint32_t            len;
} UCS_S_PACKED uct_glex_sr_req_mp_t;

typedef struct uct_glex_credit_mp {
    uct_glex_mp_hdr_t   hdr;
    uint32_t            credit_inc_ack;
    uint32_t            pad;
    void                *vc;    /* for send_credit refill on er vc */
} UCS_S_PACKED uct_glex_credit_mp_t;

typedef struct uct_glex_sr_conn_mp {
    uct_glex_mp_hdr_t   hdr;
} uct_glex_sr_conn_mp_t;

typedef struct uct_glex_er_conn_req_mp {
    uct_glex_mp_hdr_t   hdr;
    glex_mem_handle_t   mh;
    uint32_t            off;
} uct_glex_er_conn_req_mp_t;

typedef struct uct_glex_er_conn_ack_mp {
    uct_glex_mp_hdr_t   hdr;
    glex_mem_handle_t   mh;
    uint32_t            off;
} uct_glex_er_conn_ack_mp_t;

typedef struct uct_glex_er_conn_nack_mp {
    uct_glex_mp_hdr_t   hdr;
} uct_glex_er_conn_nack_mp_t;

typedef struct uct_glex_mp_info {
    void                        *data;
    uint16_t                    len;
    uint16_t                    seq_num;
    struct uct_glex_mp_info     *next;
} uct_glex_mp_info_t;

typedef struct uct_glex_flush_group {
    uct_completion_t            flush_comp; /**< Completion for outstanding requests
					         flush_comp.count is used to track
					         outstanding sends */
    uct_completion_t            *user_comp;  /**< User completion struct */
    struct uct_glex_flush_group *parent;     /**< Used to signal the next flush_group
					          that is group is done */
} uct_glex_flush_group_t;

typedef struct uct_glex_base_desc {
    glex_mem_handle_t           mh;
    uint64_t                    off;
} uct_glex_base_desc_t;

typedef uct_glex_base_desc_t uct_glex_erq_desc_t;

typedef struct uct_glex_erq_unit_hdr {
    uint16_t    recv_credit;
    uint8_t     am_id;
    uint8_t     pad;
    uint32_t    data_len;
} UCS_S_PACKED uct_glex_erq_unit_hdr_t;

/* resource and flow control status for a connection */
typedef struct uct_glex_vc {
    glex_ep_addr_t      rmt_ep_addr;
    ucs_queue_elem_t    tp_queue;       /* in the to be processed queue */

    uint16_t            send_credit;
    uint16_t            recv_credit;
    uint16_t            send_seq_num;
    uint16_t            recv_seq_num;
    int8_t              is_er;

    union {
        struct {
            ucs_queue_head_t    srq_recv_queue;
            ucs_queue_elem_t    m_queue;        /* mp scan queue */
            ucs_queue_elem_t    r_queue;        /* recv queue */
            ucs_queue_elem_t    c_queue;        /* credit queue */
            uct_glex_mp_info_t  *mp_info_list;
            uint16_t            recv_credit_refill;
            uint16_t            credit_thresh;
            uint16_t            in_mp_pending;
            uint16_t            in_recv_pending;
            uint8_t             in_credit_pending;
            uint8_t             credit_inc_ack;
            uint8_t             credit_inc_req;
            uint8_t             fc_credit_mp;
        } sr;

        struct {
            ucs_queue_elem_t    r_queue;        /* recv queue */
            uct_glex_erq_desc_t *desc;
            void                *sendq;
            void                *recvq;
            glex_mem_handle_t   rmt_mh;
            uint32_t            rmt_off;
            uint32_t            sendq_idx;
            uint32_t            recvq_idx;
            uint32_t            in_recv_pending;
            /* XXX, TODO, uint32_t *recvq_stat, but this array consume memory */
        } er;
    };
} uct_glex_vc_t;

static inline uint32_t
uct_glex_vc_hash_key_init(glex_ep_addr_t addr, uint32_t dev_id)
{
    /* XXX, TODO, how about multiple NIC, add iface->dev_id? */
    /* construct a uniq system wide hash key */
    return ((uint32_t)addr.ep_num << 24) + (uint32_t)addr.nic_id;
}

typedef struct uct_glex_srq_rdma_info {
    volatile glex_mem_handle_t  mh;
    volatile uint32_t           off;
    volatile uint32_t           srq_idx;
} UCS_S_PACKED uct_glex_srq_rdma_info_t;

typedef struct uct_glex_srq_desc {
    uct_glex_base_desc_t        base;
    ucs_queue_elem_t            queue;
    uct_glex_vc_t               *vc;
    uct_glex_flush_group_t      *flush_group;
    uint32_t                    data_len;
    uint32_t                    srq_idx;
    uint16_t                    am_id;
    uint16_t                    pad_16;
    uint32_t                    pad_32;
    /* members for RDMA PUT mode */
    uct_glex_srq_rdma_info_t     rdma_info;
} UCS_S_PACKED uct_glex_srq_desc_t;

typedef struct uct_glex_zc_desc {
    uct_glex_flush_group_t      *flush_group;
    uct_completion_t            *comp;
} uct_glex_zc_desc_t;

typedef struct uct_glex_rma_desc {
    uct_glex_base_desc_t        base;
    uct_glex_flush_group_t      *flush_group;
    uct_unpack_callback_t       unpack_cb;
    uct_completion_t            *user_comp;
    void                        *unpack_arg;
    unsigned int                length;
} uct_glex_rma_desc_t;

enum {
    UCT_GLEX_EVT_SR_SEND_DONE = 1,	/* type is in evt.cookie_1, cannot be 0 */
    UCT_GLEX_EVT_SR_RECV_DONE,
    UCT_GLEX_EVT_SR_RECV_READY,
    UCT_GLEX_EVT_ER_SEND_DONE,
    UCT_GLEX_EVT_ER_RECV_DONE,
    UCT_GLEX_EVT_RMA_PUT_DONE,
    UCT_GLEX_EVT_RMA_GET_DONE,
    UCT_GLEX_EVT_RMA_ZC_DONE
};

typedef union uct_glex_evt {
    struct {
        uint32_t        len;
        uint32_t        hash_key;
        uint8_t         type;
        uint8_t         pad_8;
        uint16_t        recv_credit;    /* XXX, TODO */
        uint32_t        idx;
    } s;
    glex_event_t v;
} UCS_S_PACKED uct_glex_evt_t;

typedef uct_glex_evt_t uct_glex_er_evt_t;
typedef uct_glex_evt_t uct_glex_sr_evt_t;
typedef uct_glex_evt_t uct_glex_rma_evt_t;

KHASH_MAP_INIT_INT(uct_glex_vc, uct_glex_vc_t*);

/**
 * Processing errors related to GLEX ops
 */
#define UCT_GLEX_TX_ERROR_RETURN(_op_name, _glex_rc, _rc, _failure) \
    { \
        if (ucs_unlikely(GLEX_SUCCESS != _glex_rc)) { \
            if (GLEX_BUSY == _glex_rc) { \
                _rc = UCS_ERR_NO_RESOURCE; \
            } else { \
                ucs_error(_op_name " failed, return: %s", \
                          glex_error_str(_glex_rc)); \
                _rc = UCS_ERR_IO_ERROR; \
            } \
            _failure; \
        } \
    }

#endif
