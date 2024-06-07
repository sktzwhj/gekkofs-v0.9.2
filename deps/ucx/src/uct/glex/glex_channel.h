/**
 * Copyright (C) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 *
 * Copyright (C) Computer Institute, NUDT, China, 2018. ALL RIGHTS RESERVED.
 */

#ifndef UCT_GLEX_CHANNEL_H
#define UCT_GLEX_CHANNEL_H

/**
 * variables defined outside of the macro: mp_req
 */
#define UCT_GLEX_FILL_AM_MP_REQ(_rmt_ep_addr, _mp) \
{ \
    mp_req.rmt_ep_addr.v = (_rmt_ep_addr).v; \
    mp_req.data          = &(_mp); \
    mp_req.len           = sizeof(_mp); \
    mp_req.flag          = 0; \
    mp_req.next          = NULL; \
}

/**
 * variables defined outside of the macro: imm_rdma_req, iface
 */
#define UCT_GLEX_FILL_AM_IMM_RDMA_REQ(_rmt_ep_addr, _data, \
                                      _rmt_mh, _rmt_off, _length) \
{ \
    imm_rdma_req.rmt_ep_addr.v = (_rmt_ep_addr).v; \
    imm_rdma_req.data          = _data; \
    imm_rdma_req.len           = _length; \
    imm_rdma_req.rmt_mh.v      = (_rmt_mh).v; \
    imm_rdma_req.rmt_offset    = _rmt_off; \
    imm_rdma_req.rmt_key       = iface->key; \
    imm_rdma_req.flag          = GLEX_FLAG_REMOTE_EVT; \
    imm_rdma_req.next          = NULL; \
}

/**
 * variables defined outside of the macro: rdma_req, iface
 */
#define UCT_GLEX_FILL_AM_RDMA_REQ(_rmt_ep_addr, _local_mh, _local_off, \
                                  _rmt_mh, _rmt_off, _length, _op) \
{ \
    rdma_req.rmt_ep_addr.v = (_rmt_ep_addr).v; \
    rdma_req.local_mh.v    = (_local_mh).v; \
    rdma_req.local_offset  = _local_off; \
    rdma_req.len           = _length; \
    rdma_req.rmt_mh.v      = (_rmt_mh).v; \
    rdma_req.rmt_offset    = _rmt_off; \
    rdma_req.type          = _op; \
    rdma_req.rmt_key       = iface->key; \
    rdma_req.flag          = GLEX_FLAG_LOCAL_EVT | GLEX_FLAG_REMOTE_EVT; \
    rdma_req.next          = NULL; \
}

/**
 * variables defined outside of the macro: rdma_req, iface, ep, evt
 */
#define UCT_GLEX_FILL_RMA_RDMA_REQ(_rmt_ep_addr, _local_mh, _local_off, \
                                   _rmt_mh, _rmt_off, _length, _op, \
                                   _evt_type, _req_idx) \
{ \
    rdma_req.rmt_ep_addr.v = (_rmt_ep_addr).v; \
    rdma_req.local_mh.v    = (_local_mh).v; \
    rdma_req.local_offset  = _local_off; \
    rdma_req.rmt_mh.v      = (_rmt_mh).v; \
    rdma_req.rmt_offset    = _rmt_off; \
    rdma_req.len           = _length; \
    rdma_req.type          = _op; \
    rdma_req.rmt_key       = iface->key; \
    rdma_req.flag          = GLEX_FLAG_LOCAL_EVT; \
    rdma_req.next          = NULL; \
    evt.s.type             = _evt_type; \
    evt.s.idx              = _req_idx; \
    rdma_req.local_evt     = evt.v; \
}

void uct_glex_sr_vc_init(uct_glex_iface_t *iface, uct_glex_vc_t *vc);
void uct_glex_er_vc_init(uct_glex_iface_t *iface, uct_glex_vc_t *vc);
void uct_glex_vc_release(uct_glex_iface_t *iface, uct_glex_vc_t *vc);

ucs_status_t uct_glex_channel_init(uct_glex_iface_t *iface);
void uct_glex_channel_cleanup(uct_glex_iface_t *iface);
void uct_glex_credit_refill(uct_glex_iface_t *iface);

unsigned uct_glex_probe_event(uct_glex_iface_t *iface);
void uct_glex_probe_mp(uct_glex_iface_t *iface);
unsigned uct_glex_sr_progress(uct_glex_iface_t *iface);
unsigned uct_glex_er_progress(uct_glex_iface_t *iface);
unsigned uct_glex_sr_mp_progress(uct_glex_iface_t *iface);

#endif
