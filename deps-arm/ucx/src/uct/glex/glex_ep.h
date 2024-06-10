/**
 * Copyright (C) UT-Battelle, LLC. 2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 *
 * Copyright (C) Computer Institute, NUDT, China, 2018. ALL RIGHTS RESERVED.
 */

#ifndef UCT_GLEX_EP_H
#define UCT_GLEX_EP_H

#include <ucs/datastruct/list.h>
#include <ucs/datastruct/arbiter.h>
#include "glex_def.h"

typedef struct uct_glex_ep {
    uct_base_ep_t           super;
    glex_ep_addr_t          rmt_ep_addr;
    uct_glex_flush_group_t  *flush_group;
    ucs_arbiter_group_t     arb_group;
    uint32_t                arb_sched;
    uct_glex_vc_t           *vc;
} uct_glex_ep_t;

UCS_CLASS_DECLARE_NEW_FUNC(uct_glex_ep_t, uct_ep_t, const uct_ep_params_t *);
UCS_CLASS_DECLARE_DELETE_FUNC(uct_glex_ep_t, uct_ep_t);

static inline int
uct_glex_ep_can_send(uct_glex_ep_t *ep)
{
    return (ucs_arbiter_group_is_empty(&ep->arb_group) || ep->arb_sched) ? 1 : 0;
}

//XXX, TODO
static inline int
uct_glex_ep_can_flush(uct_glex_ep_t *ep)
{
    return (ep->flush_group->flush_comp.count == 1 && uct_glex_ep_can_send(ep)) ? 1 : 0;
}

static inline void
uct_glex_check_flush(uct_glex_flush_group_t *flush_group)
{
    uct_invoke_completion(&flush_group->flush_comp, UCS_OK);
}

ucs_status_t uct_glex_ep_pending_add(uct_ep_h tl_ep, uct_pending_req_t *n,
				     unsigned flags);
void uct_glex_ep_pending_purge(uct_ep_h tl_ep, uct_pending_purge_callback_t cb,
		               void *arg);
ucs_arbiter_cb_result_t uct_glex_ep_process_pending(ucs_arbiter_t *arbiter,
                                                    ucs_arbiter_group_t *group,
		                                    ucs_arbiter_elem_t *elem,
						    void *arg);
ucs_arbiter_cb_result_t uct_glex_ep_arbiter_purge_cb(ucs_arbiter_t *arbiter,
                                                     ucs_arbiter_group_t *group,
		                                     ucs_arbiter_elem_t *elem,
						     void *arg);
ucs_status_t uct_glex_ep_flush(uct_ep_h tl_ep, unsigned flags,
			       uct_completion_t *comp);

ucs_status_t uct_glex_ep_put_short(uct_ep_h tl_ep, const void *buffer,
				   unsigned length, uint64_t remote_addr,
				   uct_rkey_t rkey);
ssize_t uct_glex_ep_put_bcopy(uct_ep_h tl_ep, uct_pack_callback_t pack_cb,
			      void *arg, uint64_t remote_addr, uct_rkey_t rkey);
ucs_status_t uct_glex_ep_get_bcopy(uct_ep_h tl_ep,
				   uct_unpack_callback_t unpack_cb,
				   void *arg, size_t length,
				   uint64_t remote_addr, uct_rkey_t rkey,
				   uct_completion_t *comp);
ucs_status_t uct_glex_ep_put_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov, size_t iovcnt,
                                   uint64_t remote_addr, uct_rkey_t rkey,
                                   uct_completion_t *comp);
ucs_status_t uct_glex_ep_get_zcopy(uct_ep_h tl_ep, const uct_iov_t *iov, size_t iovcnt,
                                   uint64_t remote_addr, uct_rkey_t rkey,
                                   uct_completion_t *comp);
ucs_status_t uct_glex_ep_am_short(uct_ep_h tl_ep, uint8_t id, uint64_t header,
		                  const void *payload, unsigned length);
ssize_t uct_glex_ep_am_bcopy(uct_ep_h tl_ep, uint8_t id,
		             uct_pack_callback_t pack_cb, void *arg,
			     unsigned flags);
#endif
