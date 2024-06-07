/**
 * Copyright (C) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 *
 * Copyright (C) Computer Institute, NUDT, China, 2018. ALL RIGHTS RESERVED.
 */
//#define GLEX_EP_LOG
#include <ucs/debug/log.h>
#include <ucs/arch/atomic.h>
#include <glex.h>
#include "glex_md.h"
#include "glex_iface.h"
#include "glex_ep.h"
#include "glex_channel.h"




ucs_status_t
uct_glex_ep_pending_add(uct_ep_h tl_ep, uct_pending_req_t *n, unsigned flags)
{
    uct_glex_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_glex_iface_t);
    uct_glex_ep_t *ep = ucs_derived_of(tl_ep, uct_glex_ep_t);
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    UCS_STATIC_ASSERT(sizeof(ucs_arbiter_elem_t) <= UCT_PENDING_REQ_PRIV_LEN);
    //uct_glex_enter_async(iface);
    uct_pending_req_arb_group_push(&ep->arb_group, n);
    ucs_arbiter_group_schedule(&iface->arbiter, &ep->arb_group);
    UCT_TL_EP_STAT_PEND(&ep->super);
    //uct_glex_leave_async(iface);

    return UCS_OK;
}

ucs_arbiter_cb_result_t
uct_glex_ep_process_pending(ucs_arbiter_t *arbiter,
                            ucs_arbiter_group_t *group,
		            ucs_arbiter_elem_t *elem,
			    void *arg)
{
    uct_glex_ep_t *ep = ucs_container_of(group, uct_glex_ep_t, arb_group);
    uct_pending_req_t *req = ucs_container_of(elem, uct_pending_req_t, priv);
    ucs_status_t rc;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    ep->arb_sched = 1;
    ucs_trace_data("progressing pending request %p", req);
    rc = req->func(req);
    ep->arb_sched = 0;
    ucs_trace_data("status returned from progress pending: %s",
                   ucs_status_string(rc));

    if (UCS_OK == rc) {
        /* sent scuccessfully, remove from the arbiter */
        return UCS_ARBITER_CB_RESULT_REMOVE_ELEM;
    } else if (UCS_INPROGRESS == rc) {
        return UCS_ARBITER_CB_RESULT_NEXT_GROUP;
    } else {
        /* couldn't send, keep this request in the arbiter until the next time
         * this function is called
         */
        return UCS_ARBITER_CB_RESULT_RESCHED_GROUP;
    }
}

ucs_arbiter_cb_result_t
uct_glex_ep_arbiter_purge_cb(ucs_arbiter_t *arbiter,
                             ucs_arbiter_group_t *group,
		             ucs_arbiter_elem_t *elem,
			     void *arg)
{
    uct_glex_ep_t *ep = ucs_container_of(group, uct_glex_ep_t, arb_group);
    uct_pending_req_t *req = ucs_container_of(elem, uct_pending_req_t, priv);
    uct_purge_cb_args_t *cb_args = arg;
    #ifdef GLEX_EP_LOG
    //;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    if (NULL != arg) {
        cb_args->cb(req, cb_args->arg);
    } else {
        ucs_warn("ep=%p cancelling user pending request %p", ep, req);
    }

    return UCS_ARBITER_CB_RESULT_REMOVE_ELEM;
}

void
uct_glex_ep_pending_purge(uct_ep_h tl_ep,
		          uct_pending_purge_callback_t cb,
			  void *arg)
{
    uct_glex_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_glex_iface_t);
    uct_glex_ep_t *ep = ucs_derived_of(tl_ep, uct_glex_ep_t);
    uct_purge_cb_args_t args = {cb, arg};
    #ifdef GLEX_EP_LOG
    //;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    ucs_arbiter_group_purge(&iface->arbiter, &ep->arb_group,
                            uct_glex_ep_arbiter_purge_cb, &args);
}

//XXX, TODO
static uct_glex_flush_group_t *
uct_glex_new_flush_group(uct_glex_iface_t *iface)
{
        #ifdef GLEX_EP_LOG
    //;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    return ucs_mpool_get(&iface->flush_pool);
}

static void
uct_glex_put_flush_group(uct_glex_flush_group_t *group)
{
        #ifdef GLEX_EP_LOG
    //;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    ucs_mpool_put(group);
}

static void
uct_glex_flush_cb(uct_completion_t *self)
{
    uct_glex_flush_group_t *group = ucs_container_of(self, uct_glex_flush_group_t, flush_comp);
    #ifdef GLEX_EP_LOG
    //;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    ucs_trace("group=%p, parent=%p, user_comp=%p", group, group->parent, group->user_comp);
    uct_invoke_completion(group->user_comp, UCS_OK);
    uct_glex_check_flush(group->parent);
    uct_glex_put_flush_group(group);
}

static uintptr_t
uct_glex_safe_swap_pointers(void *address, uintptr_t new_value)
{
        #ifdef GLEX_EP_LOG
    //;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    if (sizeof(void *) == 4) {
        return ucs_atomic_swap32(address, new_value);
    } else {
        return ucs_atomic_swap64(address, new_value);
    }
}

static ucs_status_t
uct_glex_add_flush_comp(uct_glex_ep_t *ep, unsigned flags,
			uct_completion_t *comp)
{
    uct_glex_iface_t *iface = ucs_derived_of(ep->super.super.iface, uct_glex_iface_t);
    uct_glex_flush_group_t *new_group, *present_group;
    #ifdef GLEX_EP_LOG
    //;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    if (!uct_glex_ep_can_send(ep)) {
        return UCS_ERR_NO_RESOURCE;
    }

    if (NULL == comp) {
        return UCS_INPROGRESS;
    }

    new_group = uct_glex_new_flush_group(iface);
    new_group->flush_comp.count = UCT_GLEX_INIT_FLUSH_REQ;
#ifdef DEBUG
    new_group->flush_comp.func = NULL;
    new_group->parent = NULL;
#endif
    present_group = (uct_glex_flush_group_t *)
                        uct_glex_safe_swap_pointers(&ep->flush_group, (uintptr_t)new_group);
    present_group->flush_comp.func = uct_glex_flush_cb;
    present_group->user_comp = comp;
    present_group->parent = new_group;
    uct_invoke_completion(&present_group->flush_comp, UCS_OK);
    return UCS_INPROGRESS;
}

ucs_status_t
uct_glex_ep_flush(uct_ep_h tl_ep, unsigned flags,
		  uct_completion_t *comp)
{
    uct_glex_ep_t *ep = ucs_derived_of(tl_ep, uct_glex_ep_t);
    ucs_status_t status = UCS_INPROGRESS;
    #ifdef GLEX_EP_LOG
    //;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    ucs_trace_func("tl_ep=%p, flags=%x, comp=%p", tl_ep, flags, comp);

    if (uct_glex_ep_can_flush(ep)) {
        UCT_TL_EP_STAT_FLUSH(ucs_derived_of(tl_ep, uct_base_ep_t));
        return UCS_OK;
    }
    status = uct_glex_add_flush_comp(ep, flags, comp);
    if (UCS_INPROGRESS == status) {
        UCT_TL_EP_STAT_FLUSH_WAIT(ucs_derived_of(tl_ep, uct_base_ep_t));
    }
    return status;
}

static ucs_status_t
uct_glex_er_connect(uct_glex_iface_t *iface,
		    uct_glex_ep_t *ep,
		    uct_glex_vc_t *vc)
{
    uct_glex_er_conn_req_mp_t req_mp;
    struct glex_imm_mp_req mp_req;
    glex_ret_t glex_rc;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    vc->er.desc = ucs_mpool_get(&iface->er.erq_pool);
    if (NULL == vc->er.desc) {
        return UCS_ERR_NO_RESOURCE;
    }

    uct_glex_er_vc_init(iface, vc);

    if (ucs_unlikely(vc->rmt_ep_addr.v == iface->address.v)) {
        /* self to self VC config: ERQ top half(sendq) --> bottom half(recvq) */
        vc->er.rmt_mh.v = vc->er.desc->mh.v;
        vc->er.rmt_off  = vc->er.desc->off + (iface->er.q_capacity << iface->er.q_unit_shift);
        vc->send_credit = iface->er.q_capacity;
        iface->sr.credit_pool += iface->glex_md->config.sr_credit_start;
        goto out;
    }

    req_mp.hdr.type = UCT_GLEX_MP_ER_CONN_REQ;
    req_mp.mh.v     = vc->er.desc->mh.v;
    req_mp.off      = vc->er.desc->off;

    UCT_GLEX_FILL_AM_MP_REQ(vc->rmt_ep_addr, req_mp);

    do {
        glex_rc = glex_send_imm_mp(iface->glex_ep, &mp_req, NULL);
    } while (GLEX_BUSY == glex_rc);
    if (ucs_unlikely(GLEX_SUCCESS != glex_rc)) {
        return UCS_ERR_IO_ERROR;
        /* XXX, TODO */
    }

out:
    iface->er.channels++;
    return UCS_OK;
}

static ucs_status_t
uct_glex_ep_connect_vc(uct_glex_iface_t *iface, uct_glex_ep_t *ep)
{
    uct_glex_vc_t *vc;
    khiter_t iter;
    uint32_t hash_key;
    int ret;
    ucs_status_t rc = UCS_OK;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    hash_key = uct_glex_vc_hash_key_init(ep->rmt_ep_addr, 0);
    iter = kh_get(uct_glex_vc, &iface->vc_hash, hash_key);

    if (iter == kh_end(&iface->vc_hash)) {
        vc = ucs_malloc(sizeof(uct_glex_vc_t), "glex_vc_t");
        if (ucs_unlikely(NULL == vc)) {
            rc = UCS_ERR_NO_RESOURCE;
            goto out;
        }

        /* store VC in hash */
        iter = kh_put(uct_glex_vc, &iface->vc_hash, hash_key, &ret);
        if (ucs_unlikely(iter == kh_end(&iface->vc_hash))) {
            ucs_free(vc);
            rc = UCS_ERR_NO_RESOURCE;
            goto out;
        }
        kh_value(&iface->vc_hash, iter) = vc;
        vc->rmt_ep_addr.v = ep->rmt_ep_addr.v;
        ep->vc = vc;

        /* try to construct er channel */
        if (iface->er.channels < iface->glex_md->config.er_max_channels) {
            if (UCS_OK == uct_glex_er_connect(iface, ep, vc)) {
                /* er_conn_req is posted, waiting for peer ack,
                 * thus the ep comm req will be add to pending queue
                */
                rc = UCS_ERR_NO_RESOURCE;
                goto out;
            }
        }

        /* use sr channel */
        uct_glex_sr_vc_init(iface, vc);
    } else {
        /* find an existed vc, remove from the to-be-connected vc queue */
        vc = kh_value(&iface->vc_hash, iter);
        ucs_queue_remove(&iface->vc_tp_queue, &vc->tp_queue);
        ep->vc = vc;
    }

out:
    return rc;
}

static UCS_CLASS_INIT_FUNC(uct_glex_ep_t, const uct_ep_params_t *params) 
{
    uct_glex_iface_t *iface = ucs_derived_of(params->iface, uct_glex_iface_t);
    glex_ep_addr_t *addr = (glex_ep_addr_t *)params->iface_addr;

    UCT_EP_PARAMS_CHECK_DEV_IFACE_ADDRS(params);
    UCS_CLASS_CALL_SUPER_INIT(uct_base_ep_t, &iface->super);
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    self->flush_group = uct_glex_new_flush_group(iface);
    self->flush_group->flush_comp.count = UCT_GLEX_INIT_FLUSH;
    self->rmt_ep_addr.v = addr->v;
    self->vc = NULL;

    self->arb_sched = 0;
    ucs_arbiter_group_init(&self->arb_group);

    return UCS_OK;
}

static UCS_CLASS_CLEANUP_FUNC(uct_glex_ep_t)
{
    uct_glex_iface_t *iface = ucs_derived_of(self->super.super.iface,
                                             uct_glex_iface_t);

    //XXX, TODO, scan srq_stat and srq_info, return mpool
     #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    ucs_arbiter_group_purge(&iface->arbiter, &self->arb_group,
                            uct_glex_ep_arbiter_purge_cb, NULL);
    uct_glex_ep_pending_purge(&self->super.super, NULL, NULL);
    if (NULL != self->vc) {
        ucs_queue_push(&iface->vc_tp_queue, &self->vc->tp_queue);
    }
    uct_glex_put_flush_group(self->flush_group);
}

UCS_CLASS_DEFINE(uct_glex_ep_t, uct_base_ep_t)
UCS_CLASS_DEFINE_NEW_FUNC(uct_glex_ep_t, uct_ep_t, const uct_ep_params_t *);
UCS_CLASS_DEFINE_DELETE_FUNC(uct_glex_ep_t, uct_ep_t);

static inline ucs_status_t
uct_glex_ep_send_direct_mp(uct_glex_iface_t *iface,
                           uct_glex_vc_t *vc,
			   uint8_t id, uint64_t header,
			   const void *payload, unsigned length,
			   int is_short)
{
    uct_glex_direct_mp_t direct_mp;
    struct glex_imm_mp_req mp_req;
    ucs_status_t rc;
    glex_ret_t glex_rc;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    direct_mp.hdr.type           = UCT_GLEX_MP_DIRECT;
    direct_mp.hdr.recv_credit    = vc->recv_credit;
    direct_mp.hdr.credit_inc_req = vc->sr.credit_inc_req;
    direct_mp.hdr.seq_num        = vc->send_seq_num;
    direct_mp.hdr.am_id          = id;

    if (is_short) {
        uct_am_short_fill_data(direct_mp.data, header, payload, length);
        length += sizeof(header);
    } else {
        memcpy(direct_mp.data, payload, length);
    }

    mp_req.rmt_ep_addr.v = vc->rmt_ep_addr.v;
    mp_req.data          = &direct_mp;
    mp_req.len           = length + sizeof(uct_glex_mp_hdr_t);
    mp_req.flag          = 0;
    mp_req.next          = NULL;

    glex_rc = glex_send_imm_mp(iface->glex_ep, &mp_req, NULL);
    UCT_GLEX_TX_ERROR_RETURN("glex_send_imm_mp", glex_rc, rc, goto err_out);

    vc->sr.credit_inc_req = 0;
    vc->recv_credit = 0;
    vc->send_seq_num++;
    vc->send_credit--;

    return UCS_OK;

err_out:
    return rc;
}

static UCS_F_ALWAYS_INLINE ucs_status_t
uct_glex_ep_er_am_short(uct_glex_iface_t *iface,
                        uct_glex_ep_t *ep,
                        uint8_t id, uint64_t header,
                        const void *payload, unsigned length)
{
    uct_glex_vc_t *vc = ep->vc;
    uct_glex_erq_unit_hdr_t *hdr;
    uct_glex_er_evt_t evt;
    unsigned int stat_idx;
    unsigned int units;
    ucs_status_t rc;
    glex_ret_t glex_rc;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    /* am_short length is always < ERQ_UNIT_SIZE */

    if ((0 == vc->send_credit)
            || ucs_unlikely(vc->er.sendq_idx > iface->er.q_thresh)) {
        return UCS_ERR_NO_RESOURCE;
    }

    hdr = vc->er.sendq + (vc->er.sendq_idx << iface->er.q_unit_shift);
    uct_am_short_fill_data(hdr + 1, header, payload, length);
    length += sizeof(header);
    hdr->recv_credit = vc->recv_credit;
    hdr->am_id       = id;
    hdr->data_len    = 0;       /* rmt evt will update data_len in recvq hdr */
    evt.s.len = length;         /* length in evt is the payload length */

    length += sizeof(uct_glex_erq_unit_hdr_t);

    if (length <= UCT_GLEX_IMM_RDMA_DATA_LEN) {
        /* XXX, TODO, use glex_md->dev_attr or config param */
        /* In glex imm_rdma_put op, local buffer can be user buffer directly,
         * but the remote buffer should be in ERQ to be received by receiver.
         * Thus the local buffer of imm_rdma_put should be also in ERQ
         * for flow control op.
         */
        struct glex_imm_rdma_req imm_rdma_req;

        UCT_GLEX_FILL_AM_IMM_RDMA_REQ(vc->rmt_ep_addr,
                                      hdr,
                                      vc->er.rmt_mh,
                                      vc->er.rmt_off +
                                        ((uintptr_t)hdr - (uintptr_t)vc->er.desc),
                                      length);

        evt.s.type     = UCT_GLEX_EVT_ER_RECV_DONE;
        evt.s.idx      = vc->er.sendq_idx;
        evt.s.hash_key = iface->hash_key;
        /* XXX, evt.s.len has been set above */
        imm_rdma_req.rmt_evt = evt.v;

        glex_rc = glex_imm_rdma(iface->glex_ep, &imm_rdma_req, NULL);
        UCT_GLEX_TX_ERROR_RETURN("glex_imm_rdma", glex_rc, rc, goto err_out);
    } else {
        struct glex_rdma_req rdma_req;
        uint32_t off;

        off = (uintptr_t)hdr - (uintptr_t)vc->er.desc;
        UCT_GLEX_FILL_AM_RDMA_REQ(vc->rmt_ep_addr,
                                  vc->er.desc->mh,
                                  vc->er.desc->off + off,
                                  vc->er.rmt_mh,
                                  vc->er.rmt_off + off,
                                  length,
                                  GLEX_RDMA_TYPE_PUT);

        stat_idx = ucs_ptr_array_insert(&iface->send_stat, ep->flush_group);
        evt.s.type         = UCT_GLEX_EVT_ER_RECV_DONE;
        evt.s.idx          = vc->er.sendq_idx;
        /* evt.s.len has been set above */
        evt.s.hash_key     = iface->hash_key;
        rdma_req.rmt_evt   = evt.v;
        evt.s.type         = UCT_GLEX_EVT_ER_SEND_DONE;
        evt.s.idx          = stat_idx;
        rdma_req.local_evt = evt.v;

        glex_rc = glex_rdma(iface->glex_ep, &rdma_req, NULL);
        UCT_GLEX_TX_ERROR_RETURN("glex_rdma", glex_rc, rc, goto err_stat);

        ++ep->flush_group->flush_comp.count;
        ++iface->outstanding;
    }

    vc->recv_credit = 0;
    vc->send_credit--;
    if (++vc->er.sendq_idx > iface->er.q_thresh) {
        /* No enough units in ERQ for one srq_unit_size,
         * remove the left units and start from the head of ERQ.
         * But in some cases, piggybacked recv_credits may be 
         * refilled after credit mp, thus should check if
         * sendq_idx can be rewinded to the head of ERQ.
         */
        units = iface->er.q_capacity - vc->er.sendq_idx;
        if (!(vc->send_credit < units)) {
            vc->send_credit -= units;
            vc->er.sendq_idx = 0;
        }
    }
    return UCS_OK;

err_stat:
    ucs_ptr_array_remove(&iface->send_stat, stat_idx);
err_out:
    return rc;
}

static UCS_F_ALWAYS_INLINE ucs_status_t
uct_glex_ep_sr_am_short(uct_glex_iface_t *iface,
			uct_glex_ep_t *ep,
			uint8_t id, uint64_t header,
			const void *payload, unsigned length)
{
    uct_glex_vc_t *vc = ep->vc;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    if (0 == vc->send_credit) {
        /* start dynamic flow control */
        if (vc->sr.credit_inc_ack) {
            vc->sr.credit_inc_req = 1;
            vc->sr.credit_inc_ack = 0;
        }
        return UCS_ERR_NO_RESOURCE;
    }

    return uct_glex_ep_send_direct_mp(iface, vc, id, header,
                                      payload, length, 1);
    /* we assume MP op is completed after it is posted to sr channel */
}

ucs_status_t
uct_glex_ep_am_short(uct_ep_h tl_ep, uint8_t id, uint64_t header,
		     const void *payload, unsigned length)
{
    uct_glex_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_glex_iface_t);
    uct_glex_ep_t *ep = ucs_derived_of(tl_ep, uct_glex_ep_t);
    ucs_status_t status;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    UCT_CHECK_AM_ID(id);
    UCT_CHECK_LENGTH(length + sizeof(header), 0,
                     UCT_GLEX_MP_DATA_LEN - sizeof(uct_glex_mp_hdr_t),
                     "am_short");

    if (ucs_unlikely(!uct_glex_ep_can_send(ep))) {
        return UCS_ERR_NO_RESOURCE;
    }

    if (ucs_unlikely(NULL == ep->vc)) {
        status = uct_glex_ep_connect_vc(iface, ep);
        if (UCS_OK != status) {
            return status;
        }
    }

    if (ep->vc->is_er) {
        return uct_glex_ep_er_am_short(iface, ep, id, header,
                                       payload, length);
    } else {
        return uct_glex_ep_sr_am_short(iface, ep, id, header,
                                       payload, length);
    }
}

static UCS_F_ALWAYS_INLINE ssize_t
uct_glex_ep_er_am_bcopy(uct_glex_iface_t *iface,
			uct_glex_ep_t *ep,
			uint8_t id,
			uct_pack_callback_t pack_cb,
			void *arg)
{
    uct_glex_vc_t *vc = ep->vc;
    struct glex_rdma_req rdma_req;
    uct_glex_er_evt_t evt;
    uct_glex_erq_unit_hdr_t *hdr;
    void *buf;
    ssize_t packed;
    unsigned int length;
    unsigned int units;
    unsigned int stat_idx;
    uint32_t off;
    ucs_status_t rc;
    glex_ret_t glex_rc;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    /* Because we only know data length after calling pack_cb(),
     * thus we must reserve maximum ERQ units for SRQ unit length here.
     */
    if ((vc->send_credit < iface->er.q_min_credits)
            || ucs_unlikely(vc->er.sendq_idx > iface->er.q_thresh)) {
        return UCS_ERR_NO_RESOURCE;
    }

    hdr = vc->er.sendq + (vc->er.sendq_idx << iface->er.q_unit_shift);
    buf = hdr + 1;
    packed = pack_cb(buf, arg);
    hdr->recv_credit = vc->recv_credit;
    hdr->am_id       = id;
    hdr->data_len    = 0;

    length = packed + sizeof(uct_glex_erq_unit_hdr_t);

    off = (uintptr_t)hdr - (uintptr_t)vc->er.desc;
    UCT_GLEX_FILL_AM_RDMA_REQ(vc->rmt_ep_addr,
                              vc->er.desc->mh,
                              vc->er.desc->off + off,
                              vc->er.rmt_mh,
                              vc->er.rmt_off + off,
                              length,
                              GLEX_RDMA_TYPE_PUT);

    stat_idx = ucs_ptr_array_insert(&iface->send_stat, ep->flush_group);
    evt.s.type         = UCT_GLEX_EVT_ER_RECV_DONE;
    evt.s.idx          = vc->er.sendq_idx;
    evt.s.len          = packed;
    evt.s.hash_key     = iface->hash_key;
    rdma_req.rmt_evt   = evt.v;
    evt.s.type         = UCT_GLEX_EVT_ER_SEND_DONE;
    evt.s.idx          = stat_idx;
    rdma_req.local_evt = evt.v;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    glex_rc = glex_rdma(iface->glex_ep, &rdma_req, NULL);
    UCT_GLEX_TX_ERROR_RETURN("glex_rdma", glex_rc, rc, goto err_stat);

    units = (length + iface->er.q_unit_mask) >> iface->er.q_unit_shift;
    vc->recv_credit  = 0;
    vc->er.sendq_idx += units;
    vc->send_credit  -= units;
    if (vc->er.sendq_idx > iface->er.q_thresh) {
        units = iface->er.q_capacity - vc->er.sendq_idx;
        if (!(vc->send_credit < units)) {
            vc->send_credit -= units;
            vc->er.sendq_idx = 0;
        }
    }

    ++ep->flush_group->flush_comp.count;
    ++iface->outstanding;

    return packed;

err_stat:
    ucs_ptr_array_remove(&iface->send_stat, stat_idx);
    return rc;
}

static UCS_F_ALWAYS_INLINE ssize_t
uct_glex_ep_sr_am_bcopy(uct_glex_iface_t *iface,
			uct_glex_ep_t *ep,
			uint8_t id,
			uct_pack_callback_t pack_cb,
			void *arg)
{
    uct_glex_vc_t *vc = ep->vc;
    uct_glex_srq_desc_t *desc;
    uct_glex_sr_req_mp_t sr_req_mp;
    struct glex_imm_mp_req mp_req;
    void *srq_buf;
    ssize_t packed;
    unsigned int srq_idx;
    ucs_status_t rc;
    glex_ret_t glex_rc;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    if (0 == vc->send_credit) {
        /* start dynamic flow control */
        if (vc->sr.credit_inc_ack) {
            vc->sr.credit_inc_req = 1;
            vc->sr.credit_inc_ack = 0;
        }
        return UCS_ERR_NO_RESOURCE;
    }

    if ((0 == iface->sr.srq_idle)
            || (iface->sr.srq_send_used == iface->sr.srq_limit)) {
        return UCS_ERR_NO_RESOURCE;
    }

    UCT_TL_IFACE_GET_TX_DESC(&iface->super, &iface->sr.free_srq_desc,
                             desc, return UCS_ERR_NO_RESOURCE);
    srq_buf = (void *)(desc + 1);
    packed = pack_cb(srq_buf, arg);

    if (packed <= (sizeof(uct_glex_direct_mp_t) - sizeof(uct_glex_mp_hdr_t))) {
        /* small AM use MP transfer directly */
        rc = uct_glex_ep_send_direct_mp(iface, vc, id, 0,
                                        srq_buf, packed, 0);
        ucs_mpool_put(desc);
        if (UCS_OK == rc) {
            UCT_TL_EP_STAT_OP(&ep->super, AM, BCOPY, packed);
            return packed;
        } else {
            return rc;
        }
    }

    srq_idx = ucs_ptr_array_insert(&iface->send_stat, desc);
    iface->sr.srq_idle--;
    iface->sr.srq_send_used++;

    sr_req_mp.hdr.type           = UCT_GLEX_MP_SR_REQ;
    sr_req_mp.hdr.recv_credit	 = vc->recv_credit;
    sr_req_mp.hdr.credit_inc_req = vc->sr.credit_inc_req;
    sr_req_mp.hdr.seq_num        = vc->send_seq_num;
    sr_req_mp.hdr.am_id          = id;
    sr_req_mp.mh.v               = desc->base.mh.v;
    sr_req_mp.off                = desc->base.off;
    sr_req_mp.srq_idx            = srq_idx;
    sr_req_mp.len                = packed;

    UCT_GLEX_FILL_AM_MP_REQ(vc->rmt_ep_addr, sr_req_mp);

    glex_rc = glex_send_imm_mp(iface->glex_ep, &mp_req, NULL);
    UCT_GLEX_TX_ERROR_RETURN("glex_send_imm_mp", glex_rc, rc, goto err_stat);

    desc->vc          = vc;
    desc->flush_group = ep->flush_group;
    desc->data_len    = packed;
    desc->srq_idx     = srq_idx;

    vc->sr.credit_inc_req = 0;
    vc->recv_credit = 0;
    vc->send_seq_num++;
    vc->send_credit--;

    ++desc->flush_group->flush_comp.count;
    ++iface->outstanding;

    UCT_TL_EP_STAT_OP(&ep->super, AM, BCOPY, packed);
    return packed;

err_stat:
    ucs_ptr_array_remove(&iface->send_stat, srq_idx);
    ucs_mpool_put(desc);
    iface->sr.srq_idle++;
    iface->sr.srq_send_used--;
    return rc;
}

ssize_t
uct_glex_ep_am_bcopy(uct_ep_h tl_ep, uint8_t id,
		     uct_pack_callback_t pack_cb, void *arg,
		     unsigned flags)
{
    uct_glex_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_glex_iface_t);
    uct_glex_ep_t *ep = ucs_derived_of(tl_ep, uct_glex_ep_t);
    ucs_status_t status;

    UCT_CHECK_AM_ID(id);
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    if (ucs_unlikely(!uct_glex_ep_can_send(ep))) {
        return UCS_ERR_NO_RESOURCE;
    }

    if (ucs_unlikely(NULL == ep->vc)) {
        status = uct_glex_ep_connect_vc(iface, ep);
        if (UCS_OK != status) {
            return status;
        }
    }

    if (ep->vc->is_er) {
        return uct_glex_ep_er_am_bcopy(iface, ep, id, pack_cb, arg);
    } else {
        return uct_glex_ep_sr_am_bcopy(iface, ep, id, pack_cb, arg);
    }
}

ucs_status_t
uct_glex_ep_put_short(uct_ep_h tl_ep, const void *buffer,
		      unsigned length, uint64_t remote_addr,
		      uct_rkey_t rkey)
{
    /* XXX, TODO, glex_imm_rdma doesn't trigger local event,
     * thus we cannot track the local completion of imm_rdma,
     * maybe we can use remote event handler to send back a
     * local completion?
     */
        #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    return UCS_ERR_UNSUPPORTED;
}

ssize_t
uct_glex_ep_put_bcopy(uct_ep_h tl_ep, uct_pack_callback_t pack_cb,
		      void *arg, uint64_t remote_addr, uct_rkey_t rkey)
{
    uct_glex_ep_t *ep = ucs_derived_of(tl_ep, uct_glex_ep_t);
    uct_glex_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_glex_iface_t);
    uct_glex_rma_desc_t *desc;
    size_t packed;
    struct glex_rdma_req rdma_req;
    uct_glex_rma_evt_t evt;
    uct_glex_key_t *key;
    unsigned int req_idx;
    ucs_status_t rc;
    glex_ret_t glex_rc;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    if ((0 == iface->rma.req_idle)
            || (!uct_glex_ep_can_send(ep))) {
        return UCS_ERR_NO_RESOURCE;
    }

    UCT_TL_IFACE_GET_TX_DESC(&iface->super, &iface->rma.free_buf_desc,
                             desc, return UCS_ERR_NO_RESOURCE);

    packed = pack_cb(desc + 1, arg);
    UCT_SKIP_ZERO_LENGTH(packed, desc);
    UCT_CHECK_LENGTH(packed, 0, iface->glex_md->config.rma_buf_size, "put_bcopy");

    req_idx = ucs_ptr_array_insert(&iface->send_stat, desc);
    iface->rma.req_idle--;

    key = (uct_glex_key_t *)rkey;
    UCT_GLEX_FILL_RMA_RDMA_REQ(ep->rmt_ep_addr,
                               desc->base.mh,
                               desc->base.off,
                               key->mh,
                               (uintptr_t)remote_addr - (uintptr_t)key->address,
                               packed,
                               GLEX_RDMA_TYPE_PUT,
                               UCT_GLEX_EVT_RMA_PUT_DONE,
                               req_idx);

    glex_rc = glex_rdma(iface->glex_ep, &rdma_req, NULL);
    UCT_GLEX_TX_ERROR_RETURN("glex_rdma", glex_rc, rc, goto err_stat);

    desc->flush_group = ep->flush_group;
    ++desc->flush_group->flush_comp.count;
    ++iface->outstanding;

    return packed;

err_stat:
    ucs_ptr_array_remove(&iface->send_stat, req_idx);
    ucs_mpool_put(desc);
    iface->rma.req_idle++;
    return rc;
}

ucs_status_t
uct_glex_ep_get_bcopy(uct_ep_h tl_ep,
		      uct_unpack_callback_t unpack_cb,
		      void *arg, size_t length,
		      uint64_t remote_addr, uct_rkey_t rkey,
		      uct_completion_t *comp)
{
    uct_glex_ep_t *ep = ucs_derived_of(tl_ep, uct_glex_ep_t);
    uct_glex_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_glex_iface_t);
    uct_glex_rma_desc_t *desc;
    struct glex_rdma_req rdma_req;
    uct_glex_rma_evt_t evt;
    uct_glex_key_t *key;
    unsigned int req_idx;
    ucs_status_t rc;
    glex_ret_t glex_rc;

    UCT_SKIP_ZERO_LENGTH(length);
    UCT_CHECK_LENGTH(length, 0, iface->glex_md->config.rma_buf_size, "get_bcopy");
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    if ((0 == iface->rma.req_idle)
            || (!uct_glex_ep_can_send(ep))) {
        return UCS_ERR_NO_RESOURCE;
    }


    UCT_TL_IFACE_GET_TX_DESC(&iface->super, &iface->rma.free_buf_desc,
                             desc, return UCS_ERR_NO_RESOURCE);

    desc->user_comp  = comp;
    desc->unpack_cb  = unpack_cb;
    desc->unpack_arg = arg;
    desc->length     = length;

    req_idx = ucs_ptr_array_insert(&iface->send_stat, desc);
    iface->rma.req_idle--;

    key = (uct_glex_key_t *)rkey;
    UCT_GLEX_FILL_RMA_RDMA_REQ(ep->rmt_ep_addr,
                               desc->base.mh,
                               desc->base.off,
                               key->mh,
                               (uintptr_t)remote_addr - (uintptr_t)key->address,
                               length,
                               GLEX_RDMA_TYPE_GET,
                               UCT_GLEX_EVT_RMA_GET_DONE,
                               req_idx);

    glex_rc = glex_rdma(iface->glex_ep, &rdma_req, NULL);
    UCT_GLEX_TX_ERROR_RETURN("glex_rdma", glex_rc, rc, goto err_stat);

    desc->flush_group = ep->flush_group;
    ++desc->flush_group->flush_comp.count;
    ++iface->outstanding;
    return UCS_INPROGRESS;

err_stat:
    ucs_ptr_array_remove(&iface->send_stat, req_idx);
    ucs_mpool_put(desc);
    iface->rma.req_idle++;
    return rc;
}

#define UCT_CHECK_PARAM_IOV(_iov, _iovcnt, _buffer, _length, _memh) \
    void     *_buffer; \
    size_t    _length; \
    uct_mem_h _memh; \
    \
    UCT_CHECK_PARAM(1 == _iovcnt, "iov[iovcnt] has to be 1 at this time"); \
    _buffer = _iov[0].buffer; \
    _length = _iov[0].length; \
    _memh   = _iov[0].memh;

static UCS_F_ALWAYS_INLINE ucs_status_t
uct_glex_ep_rdma_zcopy(uct_glex_iface_t *iface, uct_glex_ep_t *ep,
		       uct_glex_zc_desc_t *desc,
		       uint64_t buffer, size_t length, uint64_t remote_addr,
		       uct_mem_h memh, uct_rkey_t rkey,
		       uct_completion_t *comp, int opcode)
{
    uct_glex_key_t *l_key;
    uct_glex_key_t *r_key;
    struct glex_rdma_req rdma_req;
    uct_glex_rma_evt_t evt;
    unsigned int req_idx;
    ucs_status_t rc;
    glex_ret_t glex_rc;
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    if ((0 == iface->rma.zc_req_idle)
            || (!uct_glex_ep_can_send(ep))) {
        rc = UCS_ERR_NO_RESOURCE;
        goto err_out;
    }
    //printf("[%s:%ld] %s is called\n",__FILE__, __LINE__,  __FUNCTION__);

    req_idx = ucs_ptr_array_insert(&iface->send_stat, desc);
    iface->rma.zc_req_idle--;

    l_key = (uct_glex_key_t *)memh;
    r_key = (uct_glex_key_t *)rkey;

    UCT_GLEX_FILL_RMA_RDMA_REQ(ep->rmt_ep_addr,
                               l_key->mh,
                               (uintptr_t)buffer - (uintptr_t)l_key->address,
                               r_key->mh,
                               (uintptr_t)remote_addr - (uintptr_t)r_key->address,
                               length,
                               opcode,
                               UCT_GLEX_EVT_RMA_ZC_DONE,
                               req_idx);

    glex_rc = glex_rdma(iface->glex_ep, &rdma_req, NULL);
    UCT_GLEX_TX_ERROR_RETURN("glex_rdma", glex_rc, rc, goto err_stat);

    desc->comp = comp;
    desc->flush_group = ep->flush_group;
    ++desc->flush_group->flush_comp.count;
    ++iface->outstanding;

    return UCS_INPROGRESS;

err_stat:
    ucs_ptr_array_remove(&iface->send_stat, req_idx);
    iface->rma.zc_req_idle++;
err_out:
    ucs_mpool_put(desc);
    return  rc;
}

ucs_status_t
uct_glex_ep_put_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov, size_t iovcnt,
		      uint64_t remote_addr, uct_rkey_t rkey,
		      uct_completion_t *comp)
{
    uct_glex_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_glex_iface_t);
    uct_glex_ep_t *ep = ucs_derived_of(tl_ep, uct_glex_ep_t);
    uct_glex_zc_desc_t *desc;
    ucs_status_t rc;

    UCT_CHECK_PARAM_IOV(iov, iovcnt, buffer, length, memh);
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    UCT_SKIP_ZERO_LENGTH(length);
    UCT_CHECK_LENGTH(length, 0, iface->glex_md->dev_attr.max_rdma_data_len, "put_zcopy");
    UCT_TL_IFACE_GET_TX_DESC(&iface->super, &iface->rma.free_zc_desc, desc,
                             return UCS_ERR_NO_RESOURCE);

    rc = uct_glex_ep_rdma_zcopy(iface, ep, desc,
                                (uint64_t)buffer, length, (uint64_t)remote_addr,
                                memh, rkey, comp, GLEX_RDMA_TYPE_PUT);

    UCT_TL_EP_STAT_OP(ucs_derived_of(tl_ep, uct_base_ep_t), PUT, ZCOPY, length);
    return rc;
}

ucs_status_t
uct_glex_ep_get_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov, size_t iovcnt,
		      uint64_t remote_addr, uct_rkey_t rkey,
		      uct_completion_t *comp)
{
    uct_glex_iface_t *iface = ucs_derived_of(tl_ep->iface, uct_glex_iface_t);
    uct_glex_ep_t *ep = ucs_derived_of(tl_ep, uct_glex_ep_t);
    uct_glex_zc_desc_t *desc;
    ucs_status_t rc;

    UCT_CHECK_PARAM_IOV(iov, iovcnt, buffer, length, memh);
    #ifdef GLEX_EP_LOG
    ;//printf("[%s:%ld]: %s is called\n", __FILE__, __LINE__, __FUNCTION__);
    #endif
    UCT_SKIP_ZERO_LENGTH(length);
    UCT_CHECK_LENGTH(length, 0, iface->glex_md->dev_attr.max_rdma_data_len, "get_zcopy");
    UCT_TL_IFACE_GET_TX_DESC(&iface->super, &iface->rma.free_zc_desc, desc,
                             return UCS_ERR_NO_RESOURCE);

    rc = uct_glex_ep_rdma_zcopy(iface, ep, desc,
                                (uint64_t)buffer, length, (uint64_t)remote_addr,
                                memh, rkey, comp, GLEX_RDMA_TYPE_GET);
    UCT_TL_EP_STAT_OP(ucs_derived_of(tl_ep, uct_base_ep_t), GET, ZCOPY, length);
    return rc;
}
