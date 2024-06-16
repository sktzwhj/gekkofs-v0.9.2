/**
 * Copyright (C) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 *
 * Copyright (C) Computer Institute, NUDT, China, 2018-2020. ALL RIGHTS RESERVED.
 * Min Xie (xiemin@nudt.edu.cn)
 */

#include "glex_iface.h"
#include "glex_ep.h"
#include "glex_channel.h"

ucs_status_t
uct_glex_channel_init(uct_glex_iface_t *iface)
{
    uct_glex_md_t *glex_md = iface->glex_md;

    iface->credit_mp_info_list = NULL;
    iface->mp_cnt              = 0;
    iface->pending_mp_cnt      = 0;

    ucs_ptr_array_init(&iface->send_stat, "send_stat");
    ucs_ptr_array_init(&iface->sr.recv_info, "recv_srq_info");
    ucs_queue_head_init(&iface->sr.mp_vc_queue);
    ucs_queue_head_init(&iface->sr.recv_vc_queue);
    ucs_queue_head_init(&iface->sr.credit_vc_queue);
    ucs_queue_head_init(&iface->sr.send_queue);

    iface->sr.rdma_put_mode  = glex_md->config.sr_rdma_put;
    iface->sr.srq_idle       = glex_md->config.srq_capacity;
    iface->sr.srq_limit      = iface->sr.srq_idle / 4 * 3;
    iface->sr.srq_send_used  = 0;
    iface->sr.srq_recv_used  = 0;
    iface->sr.credit_inc     = glex_md->config.sr_credit_inc;
    iface->sr.credit_max     = glex_md->config.sr_credit_max;
    iface->sr.credit_pool    = glex_md->config.sr_credit_pool;

    ucs_queue_head_init(&iface->er.recv_vc_queue);
    iface->er.channels      = 0;
    iface->er.q_unit_shift  = ucs_count_trailing_zero_bits(glex_md->config.erq_unit_size);
    iface->er.q_unit_mask   = glex_md->config.erq_unit_size - 1;
    iface->er.q_min_credits = (glex_md->config.srq_unit_size + iface->er.q_unit_mask)
                                >> iface->er.q_unit_shift;
    iface->er.q_capacity    = (glex_md->config.erq_size >> iface->er.q_unit_shift) / 2;
    iface->er.q_thresh      = iface->er.q_capacity - iface->er.q_min_credits;

    iface->rma.req_idle    = glex_md->config.rma_req_capacity;
    iface->rma.zc_req_idle = glex_md->config.zc_req_capacity;

    return UCS_OK;
}

void
uct_glex_channel_cleanup(uct_glex_iface_t *iface)
{
    ucs_ptr_array_cleanup(&iface->send_stat, 1);
    ucs_ptr_array_cleanup(&iface->sr.recv_info, 1);
}

void
uct_glex_sr_vc_init(uct_glex_iface_t *iface, uct_glex_vc_t *vc)
{
    vc->send_seq_num          = 1;	/* seq_num of AM data mp of sr channel starts from 1 */
    vc->recv_seq_num          = 1;	/* can start receiving AM data mp */
    vc->send_credit           = iface->glex_md->config.sr_credit_start;
    vc->recv_credit           = 0;
    vc->is_er                 = 0;

    vc->sr.mp_info_list       = NULL;
    vc->sr.recv_credit_refill = 0;
    vc->sr.credit_thresh      = vc->send_credit;
    vc->sr.in_mp_pending      = 0;
    vc->sr.in_recv_pending    = 0;
    vc->sr.in_credit_pending  = 0;
    vc->sr.credit_inc_ack     = 1;
    vc->sr.credit_inc_req     = 0;
    vc->sr.fc_credit_mp       = 0;

    ucs_queue_head_init(&vc->sr.srq_recv_queue);
}

void
uct_glex_er_vc_init(uct_glex_iface_t *iface, uct_glex_vc_t *vc)
{
    unsigned int size;

    vc->send_credit  = 0;       /* er connection protocol is pending */
    vc->recv_credit  = 0;
    vc->send_seq_num = 0;
    vc->recv_seq_num = 0;
    vc->is_er        = 1;

    size = iface->er.q_capacity << iface->er.q_unit_shift;
    if (iface->address.v <= vc->rmt_ep_addr.v) {
        /* support send to self */
        vc->er.sendq = vc->er.desc + 1;
        vc->er.recvq = vc->er.sendq + size;
    } else {
        vc->er.recvq = vc->er.desc + 1;
        vc->er.sendq = vc->er.recvq + size;
    }
    vc->er.sendq_idx       = 0;
    vc->er.recvq_idx       = 0;
    vc->er.in_recv_pending = 0;

    /* hdr.data_len in each recvq unit is used for checking new data */
    memset(vc->er.recvq, 0, size);
}

void
uct_glex_vc_release(uct_glex_iface_t *iface, uct_glex_vc_t *vc)
{
    khiter_t iter;

    iter = kh_get(uct_glex_vc, &iface->vc_hash,
                  uct_glex_vc_hash_key_init(vc->rmt_ep_addr, 0));
    kh_del(uct_glex_vc, &iface->vc_hash, iter);
    if (vc->is_er) {
        ucs_mpool_put(vc->er.desc);
    }
    ucs_free(vc);
}

static inline void
uct_glex_evt_sr_send_done(uct_glex_iface_t *iface, uct_glex_sr_evt_t *evt)
{
    uct_glex_srq_desc_t *desc;

    if (ucs_likely(ucs_ptr_array_lookup(&iface->send_stat, evt->s.idx, desc))) {
        ucs_ptr_array_remove(&iface->send_stat, evt->s.idx);
        uct_glex_check_flush(desc->flush_group);
        iface->outstanding--;
        ucs_mpool_put(desc);
        iface->sr.srq_send_used--;
        iface->sr.srq_idle++;
    }
}

static inline void
uct_glex_evt_sr_recv_done(uct_glex_iface_t *iface, uct_glex_sr_evt_t *evt)
{
    uct_glex_srq_desc_t *desc;

    if (ucs_likely(ucs_ptr_array_lookup(&iface->sr.recv_info, evt->s.idx, desc))) {
        ucs_ptr_array_remove(&iface->sr.recv_info, evt->s.idx);
        desc->data_len = evt->s.len;

        if (0 == desc->vc->sr.in_recv_pending++) {
            ucs_queue_push(&iface->sr.recv_vc_queue, &desc->vc->sr.r_queue);
        }
    }
}

static inline void
uct_glex_evt_sr_recv_ready(uct_glex_iface_t *iface, uct_glex_sr_evt_t *evt)
{
    uct_glex_srq_desc_t *desc;

    if (ucs_likely(ucs_ptr_array_lookup(&iface->send_stat, evt->s.idx, desc))) {
        ucs_queue_push(&iface->sr.send_queue, &desc->queue);
    }
}

static inline void
uct_glex_evt_er_send_done(uct_glex_iface_t *iface, uct_glex_er_evt_t *evt)
{
    uct_glex_flush_group_t *flush_group;

    if (ucs_likely(ucs_ptr_array_lookup(&iface->send_stat, evt->s.idx, flush_group))) {
        uct_glex_check_flush(flush_group);
        ucs_ptr_array_remove(&iface->send_stat, evt->s.idx);
        iface->outstanding--;
    }
}

static inline void
uct_glex_evt_er_recv_done(uct_glex_iface_t *iface, uct_glex_er_evt_t *evt)
{
    uct_glex_vc_t *vc;
    uct_glex_erq_unit_hdr_t *hdr;
    khiter_t iter;

    iter = kh_get(uct_glex_vc, &iface->vc_hash, evt->s.hash_key);
    ucs_assert(iter != kh_end(&iface->vc_hash));
    vc = kh_value(&iface->vc_hash, iter);

    hdr = vc->er.recvq + (evt->s.idx << iface->er.q_unit_shift);
    hdr->data_len = evt->s.len;

    if (0 == vc->er.in_recv_pending++) {
        ucs_queue_push(&iface->er.recv_vc_queue, &vc->er.r_queue);
    }
}

static inline void
uct_glex_evt_rma_put_done(uct_glex_iface_t *iface, uct_glex_rma_evt_t *evt)
{
    uct_glex_rma_desc_t *desc;

    if (ucs_likely(ucs_ptr_array_lookup(&iface->send_stat, evt->s.idx, desc))) {
        ucs_ptr_array_remove(&iface->send_stat, evt->s.idx);
        uct_glex_check_flush(desc->flush_group);
        iface->outstanding--;
        ucs_mpool_put(desc);
        iface->rma.req_idle++;
    }
}

static inline void
uct_glex_evt_rma_get_done(uct_glex_iface_t *iface, uct_glex_rma_evt_t *evt)
{
    uct_glex_rma_desc_t *desc;

    if (ucs_likely(ucs_ptr_array_lookup(&iface->send_stat, evt->s.idx, desc))) {
        ucs_ptr_array_remove(&iface->send_stat, evt->s.idx);
        desc->unpack_cb(desc->unpack_arg, desc + 1, desc->length);
        if (NULL != desc->user_comp) {
            uct_invoke_completion(desc->user_comp, UCS_OK);
        }
        uct_glex_check_flush(desc->flush_group);
        iface->outstanding--;
        ucs_mpool_put(desc);
        iface->rma.req_idle++;
    }
}

static inline void
uct_glex_evt_rma_zc_done(uct_glex_iface_t *iface, uct_glex_rma_evt_t *evt)
{
    uct_glex_zc_desc_t *desc;

    if (ucs_likely(ucs_ptr_array_lookup(&iface->send_stat, evt->s.idx, desc))) {
        if (NULL != desc->comp) {
            uct_invoke_completion(desc->comp, UCS_OK);
        }

        ucs_ptr_array_remove(&iface->send_stat, evt->s.idx);
        iface->rma.zc_req_idle++;
        uct_glex_check_flush(desc->flush_group);
        iface->outstanding--;
        ucs_mpool_put(desc);
    }
}

unsigned
uct_glex_probe_event(uct_glex_iface_t *iface)
{
    glex_ep_handle_t glex_ep = iface->glex_ep;
    glex_ret_t glex_rc;
    uct_glex_evt_t *evt;
    unsigned count = 0;

    do {
        glex_rc = glex_probe_next_event(glex_ep, (glex_event_t **)&evt);
        if (GLEX_NO_EVENT == glex_rc) {
            break;
        }
        if (ucs_unlikely(GLEX_SUCCESS != glex_rc)) {
            ucs_error("Failed to probe glex event, status: %s",
                      glex_error_str(glex_rc));
            goto exit;
        }

        switch (evt->s.type) {
        case UCT_GLEX_EVT_SR_SEND_DONE:
            uct_glex_evt_sr_send_done(iface, (uct_glex_sr_evt_t *)evt);
            break;

        case UCT_GLEX_EVT_SR_RECV_DONE:
            uct_glex_evt_sr_recv_done(iface, (uct_glex_sr_evt_t *)evt);
            break;

        case UCT_GLEX_EVT_SR_RECV_READY:
            uct_glex_evt_sr_recv_ready(iface, (uct_glex_sr_evt_t *)evt);
            break;

        case UCT_GLEX_EVT_ER_SEND_DONE:
            uct_glex_evt_er_send_done(iface, (uct_glex_er_evt_t *)evt);
            break;

        case UCT_GLEX_EVT_ER_RECV_DONE:
            uct_glex_evt_er_recv_done(iface, (uct_glex_er_evt_t *)evt);
            break;

        case UCT_GLEX_EVT_RMA_PUT_DONE:
            uct_glex_evt_rma_put_done(iface, (uct_glex_rma_evt_t *)evt);
            break;

        case UCT_GLEX_EVT_RMA_GET_DONE:
            uct_glex_evt_rma_get_done(iface, (uct_glex_rma_evt_t *)evt);
            break;

        case UCT_GLEX_EVT_RMA_ZC_DONE:
            uct_glex_evt_rma_zc_done(iface, (uct_glex_rma_evt_t *)evt);
            break;

        default:
            ucs_error("Unknown event type: %d", evt->s.type);
        }

        count++;
    } while (1);

exit:
    glex_discard_probed_event(glex_ep);
    return count;
}

static uct_glex_vc_t *
uct_glex_get_vc(uct_glex_iface_t *iface, uint32_t hash_key)
{
    khiter_t iter;

    iter = kh_get(uct_glex_vc, &iface->vc_hash, hash_key);
    if (iter == kh_end(&iface->vc_hash)) {
        return NULL;
    } else {
        return kh_value(&iface->vc_hash, iter);
    }
}

static void
uct_glex_credit_mp_handler(uct_glex_iface_t *iface,
			   glex_ep_addr_t rmt_ep_addr,
			   uct_glex_credit_mp_t *mp)
{
    uct_glex_vc_t *vc;
    uct_glex_mp_info_t *mp_info;

    vc = uct_glex_get_vc(iface, uct_glex_vc_hash_key_init(rmt_ep_addr, 0));
    ucs_assert(NULL != vc);

    if (!vc->is_er) {
        if (mp->credit_inc_ack) {
            vc->sr.credit_inc_ack = 1;
        }
    }

    /* send_credit of vc should be refilled after discarding probed mp,
     * to prevent multiple credit MPs from the same vc in MPQ
     */
    mp_info = ucs_mpool_get(&iface->mp_info);
    if (ucs_unlikely(NULL == mp_info)) {
        ucs_error("Failed to get mp_info");
        return; /* XXX, TODO */
    }

    mp->vc = vc;
    mp_info->data = mp;
    if (NULL == iface->credit_mp_info_list) {
        iface->credit_mp_info_list = mp_info;
        mp_info->next = NULL;
    } else {
        mp_info->next = iface->credit_mp_info_list;
        iface->credit_mp_info_list = mp_info;
    }
}

//XXX, TODO, error return?
static void
uct_glex_er_conn_req_handler(uct_glex_iface_t *iface,
			     glex_ep_addr_t rmt_ep_addr,
			     uct_glex_er_conn_req_mp_t *mp)
{
    uct_glex_vc_t *vc;
    uct_glex_er_conn_nack_mp_t nack_mp;
    uct_glex_er_conn_ack_mp_t ack_mp;
    struct glex_imm_mp_req mp_req;
    uint32_t hash_key;
    khiter_t iter;
    int ret;
    glex_ret_t glex_rc;

    hash_key = uct_glex_vc_hash_key_init(rmt_ep_addr, 0);
    vc = uct_glex_get_vc(iface, hash_key);
    if (NULL == vc) {
        vc = ucs_malloc(sizeof(uct_glex_vc_t), "glex_vc_t");
        ucs_assert_always(NULL != vc);
        iter = kh_put(uct_glex_vc, &iface->vc_hash, hash_key, &ret);
        ucs_assert_always(iter != kh_end(&iface->vc_hash));
        kh_value(&iface->vc_hash, iter) = vc;
        vc->rmt_ep_addr.v = rmt_ep_addr.v;
        /* the new created vc is in the to-be-connected status */
        ucs_queue_push(&iface->vc_tp_queue, &vc->tp_queue);

        if (iface->er.channels == iface->glex_md->config.er_max_channels) {
            goto use_sr_channel;
        }

        vc->er.desc = ucs_mpool_get(&iface->er.erq_pool);
        if (NULL == vc->er.desc) {
            goto use_sr_channel;
        }

        uct_glex_er_vc_init(iface, vc);
        vc->er.rmt_mh.v = mp->mh.v;
        vc->er.rmt_off  = mp->off;
        /* set send_credit, er vc can start rdma transfer now */
        vc->send_credit = iface->er.q_capacity;
        iface->er.channels++;
        iface->sr.credit_pool += iface->glex_md->config.sr_credit_start;

        ack_mp.hdr.type = UCT_GLEX_MP_ER_CONN_ACK;
        ack_mp.mh.v     = vc->er.desc->mh.v;
        ack_mp.off      = vc->er.desc->off;

        UCT_GLEX_FILL_AM_MP_REQ(vc->rmt_ep_addr, ack_mp);
        goto send_mp;
    } else {
        if (vc->is_er) {
            /* peer posts er_conn_req at the same time */
            vc->er.rmt_mh.v = mp->mh.v;
            vc->er.rmt_off  = mp->off;
            vc->send_credit = iface->er.q_capacity;
            iface->sr.credit_pool += iface->glex_md->config.sr_credit_start;
            return;
        } else {
            goto do_nack;
        }
    }

use_sr_channel:
    uct_glex_sr_vc_init(iface, vc);

do_nack:
    nack_mp.hdr.type = UCT_GLEX_MP_ER_CONN_NACK;
    UCT_GLEX_FILL_AM_MP_REQ(vc->rmt_ep_addr, nack_mp);
send_mp:
    do {
        glex_rc = glex_send_imm_mp(iface->glex_ep, &mp_req, NULL);
    } while (GLEX_BUSY == glex_rc);
    if (ucs_unlikely(GLEX_SUCCESS != glex_rc)) {
        /* XXX, TODO */
    }
}

static void
uct_glex_er_conn_ack_handler(uct_glex_iface_t *iface,
			     glex_ep_addr_t rmt_ep_addr,
			     uct_glex_er_conn_ack_mp_t *mp)
{
    uct_glex_vc_t *vc;

    vc = uct_glex_get_vc(iface, uct_glex_vc_hash_key_init(rmt_ep_addr, 0));
    ucs_assert(NULL != vc);
    vc->er.rmt_mh.v = mp->mh.v;
    vc->er.rmt_off  = mp->off;
    vc->send_credit = iface->er.q_capacity;
    iface->sr.credit_pool += iface->glex_md->config.sr_credit_start;
}

static void
uct_glex_er_conn_nack_handler(uct_glex_iface_t *iface,
			      glex_ep_addr_t rmt_ep_addr,
			      uct_glex_er_conn_nack_mp_t *mp)
{
    uct_glex_vc_t *vc;

    vc = uct_glex_get_vc(iface, uct_glex_vc_hash_key_init(rmt_ep_addr, 0));
    ucs_assert(NULL != vc);
    if (vc->is_er) {
        ucs_mpool_put(vc->er.desc);
        iface->er.channels--;
        uct_glex_sr_vc_init(iface, vc);
    }
    /* else, the VC has been changed to SR VC by a SR MP */
}

//XXX, TODO, return value
static void
uct_glex_recv_protocol_mp(uct_glex_iface_t *iface,
                          glex_ep_addr_t rmt_ep_addr,
                          uct_glex_mp_hdr_t *hdr)
{
    switch (hdr->type) {
    case UCT_GLEX_MP_CREDIT:
        uct_glex_credit_mp_handler(iface, rmt_ep_addr,
                                   (uct_glex_credit_mp_t *)hdr);
        break;

    case UCT_GLEX_MP_ER_CONN_REQ:
        uct_glex_er_conn_req_handler(iface, rmt_ep_addr,
                                     (uct_glex_er_conn_req_mp_t *)hdr);
        break;

    case UCT_GLEX_MP_ER_CONN_ACK:
        uct_glex_er_conn_ack_handler(iface, rmt_ep_addr,
                                     (uct_glex_er_conn_ack_mp_t *)hdr);
        break;

    case UCT_GLEX_MP_ER_CONN_NACK:
        uct_glex_er_conn_nack_handler(iface, rmt_ep_addr,
                                      (uct_glex_er_conn_nack_mp_t *)hdr);
        break;
    }
}

static UCS_F_ALWAYS_INLINE int
seq_num_before(uct_glex_vc_t *vc, uint16_t seq_0, uint16_t seq_1)
{
    if (seq_0 < seq_1) {
        if (seq_0 < vc->recv_seq_num) {
            if (seq_1 < vc->recv_seq_num) {
                return 1;
            } else {
                return 0;
            }
        } else {
            return 1;
        }
    } else {
        if (seq_1 < vc->recv_seq_num) {
            if (seq_0 < vc->recv_seq_num) {
                return 0;
            } else {
                return 1;
            }
        } else {
            return 0;
        }
    }
}

static void
uct_glex_mp_add_to_vc_sort_list(uct_glex_iface_t *iface,
                                glex_ep_addr_t rmt_ep_addr,
                                void *mp_data, uint32_t mp_len)
{
    uct_glex_vc_t *vc;
    uct_glex_mp_hdr_t *mp_hdr;
    uct_glex_mp_info_t *mp_info, *c_mp_info, *n_mp_info;
    uint16_t mp_seq_num, list_head_seq_num;
    uint32_t hash_key;
    khiter_t iter;
    int ret;

    mp_hdr = (uct_glex_mp_hdr_t *)mp_data;

    mp_info = ucs_mpool_get(&iface->mp_info);
    if (ucs_unlikely(NULL == mp_info)) {
        ucs_error("Failed to get mp_info");
        return; /* XXX, TODO, this MP is lost */
    }

    mp_info->data    = mp_data;
    mp_info->len     = mp_len;
    mp_info->seq_num = mp_hdr->seq_num;

    hash_key = uct_glex_vc_hash_key_init(rmt_ep_addr, 0);
    iter = kh_get(uct_glex_vc, &iface->vc_hash, hash_key);
    if (ucs_likely(iter != kh_end(&iface->vc_hash))) {
        vc = kh_value(&iface->vc_hash, iter);
        if (ucs_unlikely(vc->is_er)) {
            /* this is the first SR MP, but there is an ER VC
             * in connecting status, change it to SR VC
             */
            ucs_mpool_put(vc->er.desc);
            iface->er.channels--;
            uct_glex_sr_vc_init(iface, vc);
        }
    } else {
        /* create a SR VC */
        vc = ucs_malloc(sizeof(uct_glex_vc_t), "glex_vc_t");
        ucs_assert_always(NULL != vc);
        vc->rmt_ep_addr.v = rmt_ep_addr.v;
        iter = kh_put(uct_glex_vc, &iface->vc_hash, hash_key, &ret);
        ucs_assert_always(iter != kh_end(&iface->vc_hash));
        kh_value(&iface->vc_hash, iter) = vc;
        ucs_queue_push(&iface->vc_tp_queue, &vc->tp_queue);
        uct_glex_sr_vc_init(iface, vc);
    }

    /* add mp to VC's sorted mp_info_list */
    mp_seq_num = mp_info->seq_num;
    if (NULL == vc->sr.mp_info_list) {
        vc->sr.mp_info_list = mp_info;
        mp_info->next = NULL;
        list_head_seq_num = mp_seq_num;
        goto exit;
    }

    c_mp_info = vc->sr.mp_info_list;
    if (seq_num_before(vc, mp_seq_num, c_mp_info->seq_num)) {
        mp_info->next = vc->sr.mp_info_list;
        vc->sr.mp_info_list = mp_info;
        list_head_seq_num = mp_seq_num;
        goto exit;
    } else {
        list_head_seq_num = c_mp_info->seq_num;
    }

    while (1) {
        n_mp_info = c_mp_info->next;
        if (NULL == n_mp_info) {
            c_mp_info->next = mp_info;
            mp_info->next = NULL;
            break;
        }

        if (seq_num_before(vc, mp_seq_num, n_mp_info->seq_num)) {
            mp_info->next = n_mp_info;
            c_mp_info->next = mp_info;
            break;
        }
        c_mp_info = n_mp_info;
    }

exit:
    if ((0 == vc->sr.in_mp_pending)
            && (list_head_seq_num == vc->recv_seq_num)) {
        ucs_queue_push(&iface->sr.mp_vc_queue, &vc->sr.m_queue);
        vc->sr.in_mp_pending = 1;
    }
}

void
uct_glex_probe_mp(uct_glex_iface_t *iface)
{
    glex_ep_handle_t glex_ep = iface->glex_ep;
    glex_ep_addr_t rmt_ep_addr;
    char *mp_data;
    uct_glex_mp_hdr_t *mp_hdr;
    uint32_t mp_len;
    glex_ret_t glex_rc;

    do {
        glex_rc = glex_probe_next_mp(glex_ep, &rmt_ep_addr,
                                     (void **)&mp_data, &mp_len);
        if (GLEX_NO_MP == glex_rc) {
            break;
        }
        if (ucs_unlikely(GLEX_SUCCESS != glex_rc)) {
            ucs_error("Failed to probe glex mp, status: %s",
                      glex_error_str(glex_rc));
            break; /* XXX, TODO, return error */
        }

        iface->mp_cnt++;
        mp_hdr = (uct_glex_mp_hdr_t *)mp_data;
        if (mp_hdr->type > UCT_GLEX_MP_SR_REQ) {
            uct_glex_recv_protocol_mp(iface, rmt_ep_addr, mp_hdr);
            continue;
        }

        /* MP DIRECT and SR_REQ will be queued and sorted */
        iface->pending_mp_cnt++;
        uct_glex_mp_add_to_vc_sort_list(iface, rmt_ep_addr, mp_data, mp_len);
    } while (1);
}

static void
uct_glex_credit_flow_control(uct_glex_iface_t *iface,
			     uct_glex_vc_t *vc,
			     uct_glex_mp_hdr_t *mp_hdr)
{
    if (0 == vc->sr.in_credit_pending) {
        ucs_queue_push(&iface->sr.credit_vc_queue, &vc->sr.c_queue);
        vc->sr.in_credit_pending = 1;
    }
    vc->sr.recv_credit_refill++;

    vc->send_credit += mp_hdr->recv_credit;
    if (mp_hdr->recv_credit) {
        /* when some credits are piggybacked,
         * stop the flow control request in sender
         */
        vc->sr.credit_inc_req = 0;
        vc->sr.credit_inc_ack = 1;
    }

    if (mp_hdr->credit_inc_req) {
        vc->sr.fc_credit_mp = 1;
    }
}

static UCS_F_ALWAYS_INLINE void
uct_glex_mp_direct_handler(uct_glex_iface_t *iface, uct_glex_mp_hdr_t *mp_hdr,
			   unsigned int len)
{
    void *user_data;

    len -= sizeof(uct_glex_mp_hdr_t);
    user_data = (void *)(mp_hdr + 1);
    uct_iface_trace_am(&iface->super, UCT_AM_TRACE_TYPE_RECV,
                       mp_hdr->am_id, user_data, len,
                       "RX: AM");
    uct_iface_invoke_am(&iface->super, mp_hdr->am_id,
                        user_data, len, 0);
}

static inline glex_ret_t
uct_glex_mp_sr_req_get_mode(uct_glex_iface_t *iface,
                            uct_glex_srq_desc_t *desc,
                            uct_glex_sr_req_mp_t *mp,
                            uint32_t srq_idx)
{
    struct glex_rdma_req rdma_req;
    uct_glex_sr_evt_t evt;

    UCT_GLEX_FILL_AM_RDMA_REQ(desc->vc->rmt_ep_addr,
                              desc->base.mh,
                              desc->base.off,
                              mp->mh,
                              mp->off,
                              mp->len,
                              GLEX_RDMA_TYPE_GET);

    evt.s.type         = UCT_GLEX_EVT_SR_RECV_DONE;
    evt.s.idx          = srq_idx;
    evt.s.len          = mp->len;
    rdma_req.local_evt = evt.v;
    evt.s.type         = UCT_GLEX_EVT_SR_SEND_DONE;
    evt.s.idx          = mp->srq_idx;
    rdma_req.rmt_evt   = evt.v;

    return glex_rdma(iface->glex_ep, &rdma_req, NULL);
}

static inline glex_ret_t
uct_glex_mp_sr_req_put_mode(uct_glex_iface_t *iface,
                            uct_glex_srq_desc_t *desc,
                            uct_glex_sr_req_mp_t *mp,
                            uint32_t srq_idx)
{
    struct glex_imm_rdma_req imm_rdma_req;
    uct_glex_sr_evt_t evt;
    uct_glex_srq_rdma_info_t rdma_info;

    /* write back the recv srq info to sender using IMM RDMA PUT */
    rdma_info.mh.v    = desc->base.mh.v;
    rdma_info.off     = desc->base.off;
    rdma_info.srq_idx = srq_idx;

    UCT_GLEX_FILL_AM_IMM_RDMA_REQ(desc->vc->rmt_ep_addr,
                                  &rdma_info,
                                  mp->mh,
                                  mp->off - sizeof(uct_glex_srq_desc_t)
                                    + ucs_offsetof(uct_glex_srq_desc_t, rdma_info),
                                  sizeof(uct_glex_srq_rdma_info_t));

    evt.s.type = UCT_GLEX_EVT_SR_RECV_READY;
    evt.s.idx  = mp->srq_idx;
    imm_rdma_req.rmt_evt = evt.v;

    return glex_imm_rdma(iface->glex_ep, &imm_rdma_req, NULL);
}

//XXX, TODO, change to inline
static ucs_status_t
uct_glex_mp_sr_req_handler(uct_glex_iface_t *iface,
			   uct_glex_vc_t *vc,
			   uct_glex_sr_req_mp_t *mp)
{
    uct_glex_srq_desc_t *desc;
    uint32_t srq_idx;
    ucs_status_t rc;
    glex_ret_t glex_rc;

    UCT_TL_IFACE_GET_RX_DESC(&iface->super, &iface->sr.free_srq_desc,
                             desc, return UCS_ERR_NO_RESOURCE);

    desc->vc       = vc;
    desc->data_len = 0;
    desc->am_id    = mp->hdr.am_id;

    srq_idx = ucs_ptr_array_insert(&iface->sr.recv_info, desc);
    iface->sr.srq_idle--;
    iface->sr.srq_recv_used++;

    if (iface->sr.rdma_put_mode) {
        glex_rc = uct_glex_mp_sr_req_put_mode(iface, desc, mp, srq_idx);
    } else {
        glex_rc = uct_glex_mp_sr_req_get_mode(iface, desc, mp, srq_idx);
    }
    UCT_GLEX_TX_ERROR_RETURN("glex_rdma", glex_rc, rc, goto err_stat);

    ucs_queue_push(&vc->sr.srq_recv_queue, &desc->queue);

    ++iface->outstanding;       //XXX, TODO, flush is not needed in AM?

    return UCS_OK;

err_stat:
    ucs_ptr_array_remove(&iface->sr.recv_info, srq_idx);
    ucs_mpool_put(desc);
    iface->sr.srq_idle++;
    iface->sr.srq_recv_used--;
    return rc;
}

static unsigned
uct_glex_sr_recv_queued_mp(uct_glex_iface_t *iface, uct_glex_vc_t *vc)
{
    uct_glex_mp_info_t *mp_info;
    uct_glex_mp_hdr_t *mp_hdr;
    unsigned count = 0;

    do {
        mp_info = vc->sr.mp_info_list;
        if (mp_info->seq_num != vc->recv_seq_num) {
            break;
        }

        mp_hdr = (uct_glex_mp_hdr_t *)mp_info->data;
        switch (mp_hdr->type) {
        case UCT_GLEX_MP_DIRECT:
            /* To ensure the order of data, when there are
             * pending srq RDMA, direct mp cannot be received.
             */
            if (!ucs_queue_is_empty(&vc->sr.srq_recv_queue)) {
                goto exit;
            }

            uct_glex_mp_direct_handler(iface, mp_hdr,
                                       (unsigned int)mp_info->len);
            break;

	case UCT_GLEX_MP_SR_REQ:
            if ((0 == iface->sr.srq_idle)
                    || (iface->sr.srq_recv_used == iface->sr.srq_limit)) {
                goto exit;
            }

            if (uct_glex_mp_sr_req_handler(iface, vc,
                                           (uct_glex_sr_req_mp_t *)mp_hdr)
                    != UCS_OK) {
                goto exit;
            }
            break;
        }

        /* prepare credit refill and flow control */
        uct_glex_credit_flow_control(iface, vc, mp_hdr);

        vc->recv_seq_num++;
        iface->pending_mp_cnt--;

        /* free received mp_info */
        vc->sr.mp_info_list = mp_info->next;
        ucs_mpool_put(mp_info);

        count++;
    } while (vc->sr.mp_info_list);

exit:
    return count;
}

//XXX, TODO, return value
static ucs_status_t
uct_glex_sr_send_credit_mp(uct_glex_iface_t *iface, uct_glex_vc_t *vc)
{
    uct_glex_credit_mp_t credit_mp;
    struct glex_imm_mp_req mp_req;
    glex_ret_t glex_rc;
    int new_credits;

    vc->recv_credit += vc->sr.recv_credit_refill;
    vc->sr.recv_credit_refill = 0;

    if (vc->sr.fc_credit_mp) {
        /* sender needs more credits, get new credits from credit pool */
        new_credits = ucs_min(iface->sr.credit_inc,
                              iface->sr.credit_max - vc->sr.credit_thresh);
        new_credits = ucs_min(new_credits, iface->sr.credit_pool);
        iface->sr.credit_pool -= new_credits;

        credit_mp.hdr.type           = UCT_GLEX_MP_CREDIT;
        credit_mp.hdr.credit_inc_req = 0;
        credit_mp.hdr.recv_credit    = vc->recv_credit + new_credits;
        credit_mp.credit_inc_ack     = 1;

        UCT_GLEX_FILL_AM_MP_REQ(vc->rmt_ep_addr, credit_mp);

        do {
            glex_rc = glex_send_imm_mp(iface->glex_ep, &mp_req, NULL);
        } while (GLEX_BUSY == glex_rc);

        vc->recv_credit = 0;
        vc->sr.credit_thresh += new_credits;
        if (ucs_unlikely(GLEX_SUCCESS != glex_rc)) {
            ucs_error("glex_send_imm_mp failed, return: %s",
                      glex_error_str(glex_rc));
            /* XXX, TODO, keep vc in credit scan list? */
        }

        goto exit;
    }

    if (vc->recv_credit == vc->sr.credit_thresh) {
        credit_mp.hdr.type           = UCT_GLEX_MP_CREDIT;
        credit_mp.hdr.credit_inc_req = 0;
        credit_mp.hdr.recv_credit    = vc->recv_credit;
        credit_mp.credit_inc_ack     = 0;

        UCT_GLEX_FILL_AM_MP_REQ(vc->rmt_ep_addr, credit_mp);

        do {
           glex_rc = glex_send_imm_mp(iface->glex_ep, &mp_req, NULL);
        } while (GLEX_BUSY == glex_rc);
        vc->recv_credit = 0;
        if (ucs_unlikely(GLEX_SUCCESS != glex_rc)) {
            ucs_error("glex_send_imm_mp failed, return: %s",
                      glex_error_str(glex_rc));
            /* XXX, TODO, keep vc in credit scan list? */
        }	
    }

exit:
    vc->sr.fc_credit_mp = 0;    /* XXX, TODO, error condition */
    vc->sr.in_credit_pending = 0;
    return UCS_OK;
}

static ucs_status_t
uct_glex_sr_vc_recv_progress(uct_glex_iface_t *iface, uct_glex_vc_t *vc)
{
    uct_glex_srq_desc_t *desc;
    ucs_status_t rc;

    while (!ucs_queue_is_empty(&vc->sr.srq_recv_queue)) {
        desc = ucs_queue_head_elem_non_empty(&vc->sr.srq_recv_queue,
                                             uct_glex_srq_desc_t,
                                             queue);
        if (0 == desc->data_len) {
            /* RDMA GET request should be processed in order */
            break;
        }

        uct_iface_trace_am(&iface->super, UCT_AM_TRACE_TYPE_RECV,
                           desc->am_id, desc+1, desc->data_len,
                           "RX: AM");
        rc = uct_iface_invoke_am(&iface->super, desc->am_id,
                                 desc+1, desc->data_len, 0);
        if (UCS_OK == rc) {
            ucs_queue_pull_non_empty(&vc->sr.srq_recv_queue);
            iface->sr.srq_idle++;
            iface->sr.srq_recv_used--;
            iface->outstanding--;
            vc->sr.in_recv_pending--;
            ucs_mpool_put(desc);
	} else {
            return rc; //XXX, TODO
	}
    }

    return UCS_OK;
}

/* send progress in RDMA PUT mode */
static ucs_status_t
uct_glex_sr_send_progress(uct_glex_iface_t *iface)
{
    uct_glex_srq_desc_t *desc;
    struct glex_rdma_req rdma_req;
    uct_glex_sr_evt_t evt;
    ucs_status_t rc;
    glex_ret_t glex_rc;

    do {
        desc = ucs_queue_head_elem_non_empty(&iface->sr.send_queue,
                                             uct_glex_srq_desc_t,
                                             queue);

        UCT_GLEX_FILL_AM_RDMA_REQ(desc->vc->rmt_ep_addr,
                                  desc->base.mh,
                                  desc->base.off,
                                  desc->rdma_info.mh,
                                  desc->rdma_info.off,
                                  desc->data_len,
                                  GLEX_RDMA_TYPE_PUT);

        evt.s.type = UCT_GLEX_EVT_SR_SEND_DONE;
        evt.s.idx  = desc->srq_idx;
        rdma_req.local_evt = evt.v;
        evt.s.type = UCT_GLEX_EVT_SR_RECV_DONE;
        evt.s.idx  = desc->rdma_info.srq_idx;
        evt.s.len  = desc->data_len;
        rdma_req.rmt_evt = evt.v;

        glex_rc = glex_rdma(iface->glex_ep, &rdma_req, NULL);
        UCT_GLEX_TX_ERROR_RETURN("glex_rdma", glex_rc, rc, goto err_stat);

        ucs_queue_pull_non_empty(&iface->sr.send_queue);
    } while (!ucs_queue_is_empty(&iface->sr.send_queue));

    return UCS_OK;

err_stat:
    return rc;
}

static ucs_status_t
uct_glex_sr_recv_progress(uct_glex_iface_t *iface)
{
    uct_glex_vc_t *vc;
    ucs_queue_iter_t iter;
    ucs_status_t rc;

    ucs_queue_for_each_safe(vc, iter, &iface->sr.recv_vc_queue, sr.r_queue) {
        rc = uct_glex_sr_vc_recv_progress(iface, vc);
        if (0 == vc->sr.in_recv_pending) {
            ucs_queue_del_iter(&iface->sr.recv_vc_queue, iter);
        }
    }

    return rc;  //XXX, TODO, return count?
}


unsigned
uct_glex_sr_mp_progress(uct_glex_iface_t *iface)
{
    uct_glex_vc_t *vc;
    ucs_queue_iter_t iter;
    unsigned count = 0;

    ucs_queue_for_each_safe(vc, iter, &iface->sr.mp_vc_queue, sr.m_queue) {
        count += uct_glex_sr_recv_queued_mp(iface, vc);
        if ((NULL == vc->sr.mp_info_list)
            || (vc->sr.mp_info_list->seq_num != vc->recv_seq_num)) {
            ucs_queue_del_iter(&iface->sr.mp_vc_queue, iter);
            vc->sr.in_mp_pending = 0;
        }
    }

    return count;
}

unsigned
uct_glex_sr_progress(uct_glex_iface_t *iface)
{
    unsigned count = 0; //XXX, TODO

    if (!ucs_queue_is_empty(&iface->sr.send_queue)) {
        uct_glex_sr_send_progress(iface);
    }
    if (!ucs_queue_is_empty(&iface->sr.recv_vc_queue)) {
        uct_glex_sr_recv_progress(iface); /* XXX, TODO, check return code */
    }

    return count;
}

static void
uct_glex_er_send_credit_mp(uct_glex_iface_t *iface, uct_glex_vc_t *vc)
{
    uct_glex_credit_mp_t credit_mp;
    struct glex_imm_mp_req mp_req;
    glex_ret_t glex_rc;

    credit_mp.hdr.type           = UCT_GLEX_MP_CREDIT;
    credit_mp.hdr.credit_inc_req = 0;
    credit_mp.hdr.recv_credit    = vc->recv_credit;
    credit_mp.credit_inc_ack     = 0;

    UCT_GLEX_FILL_AM_MP_REQ(vc->rmt_ep_addr, credit_mp);

    do {
        glex_rc = glex_send_imm_mp(iface->glex_ep, &mp_req, NULL);
    } while (GLEX_BUSY == glex_rc);
    if (ucs_unlikely(GLEX_SUCCESS != glex_rc)) {
        ucs_error("glex_send_imm_mp failed, return: %s",
                  glex_error_str(glex_rc));  //XXX, TODO, what info to print
    }
    vc->recv_credit = 0;
}

static inline ucs_status_t
uct_glex_er_vc_recv_progress(uct_glex_iface_t *iface, uct_glex_vc_t *vc)
{
    uct_glex_erq_unit_hdr_t *hdr;
    unsigned int units;
    unsigned int i;
    uint32_t q_capacity = iface->er.q_capacity;
    uint32_t unit_shift = iface->er.q_unit_shift;
    uint32_t unit_mask = iface->er.q_unit_mask;
    uint32_t recvq_idx = vc->er.recvq_idx;
    uint16_t recv_credit = 0;

again:
    hdr = vc->er.recvq + (recvq_idx << unit_shift);

    while ((recvq_idx < q_capacity) && hdr->data_len) {
        uct_iface_trace_am(&iface->super, UCT_AM_TRACE_TYPE_RECV,
                           hdr->am_id, hdr+1, hdr->data_len,
                           "RX: AM");
        uct_iface_invoke_am(&iface->super, hdr->am_id,
                            hdr+1, hdr->data_len, 0);

        if (hdr->recv_credit) {
            /* piggybacked recv_credit may be processed after credit mp */
            vc->send_credit += hdr->recv_credit;
            if (ucs_unlikely(vc->er.sendq_idx > iface->er.q_thresh)) {
                units = q_capacity - vc->er.sendq_idx;
                if (!(vc->send_credit < units)) {
                    vc->send_credit -= units;
                    vc->er.sendq_idx = 0;
                }
            }
        }
        --vc->er.in_recv_pending;

        units = ((sizeof(uct_glex_erq_unit_hdr_t) + hdr->data_len)
                + unit_mask) >> unit_shift;
        /* recycle the received erq units */
        for (i = 0; i < units; i++, recvq_idx++, recv_credit++) {
            hdr->data_len = 0;
            hdr = (void *)hdr + (1 << unit_shift);
        }
    }

    if (recvq_idx > iface->er.q_thresh) {
        recv_credit += q_capacity - recvq_idx;
        recvq_idx = 0;
        /* maybe sender has transfered new data to the start of erq */
        goto again;
    }

    vc->er.recvq_idx = recvq_idx;
    vc->recv_credit += recv_credit;

    if (vc->recv_credit > iface->er.q_thresh) {
        uct_glex_er_send_credit_mp(iface, vc);
    }

    return UCS_OK;
}

unsigned
uct_glex_er_progress(uct_glex_iface_t *iface)
{
    uct_glex_vc_t *vc;
    ucs_queue_iter_t iter;
    unsigned count = 0;

    ucs_queue_for_each_safe(vc, iter, &iface->er.recv_vc_queue, er.r_queue) {
        uct_glex_er_vc_recv_progress(iface, vc);
        if (0 == vc->er.in_recv_pending) {
            ucs_queue_del_iter(&iface->er.recv_vc_queue, iter);
            count++;
        }
    }

    return count;
}

static void
uct_glex_credit_mp_refill(uct_glex_iface_t *iface)
{
    uct_glex_mp_info_t *mp_info;
    uct_glex_credit_mp_t *mp;

    do {
        mp_info = iface->credit_mp_info_list;
        mp = (uct_glex_credit_mp_t *)mp_info->data;
        ((uct_glex_vc_t *)(mp->vc))->send_credit += mp->hdr.recv_credit;

        iface->credit_mp_info_list = mp_info->next;
        ucs_mpool_put(mp_info);
    } while (iface->credit_mp_info_list);
}

//XXX, TODO, return value
void
uct_glex_credit_refill(uct_glex_iface_t *iface)
{
    uct_glex_vc_t *vc;
    ucs_queue_iter_t iter;

    if (iface->mp_cnt && (0 == iface->pending_mp_cnt)) {
        if (iface->credit_mp_info_list) {
            uct_glex_credit_mp_refill(iface);
        }

        iface->mp_cnt = 0;
        glex_discard_probed_mp(iface->glex_ep);

        ucs_queue_for_each_safe(vc, iter, &iface->sr.credit_vc_queue, sr.c_queue) {
            uct_glex_sr_send_credit_mp(iface, vc);
            ucs_queue_del_iter(&iface->sr.credit_vc_queue, iter);
        }
    }
}

