/**
 * Copyright (c) UT-Battelle, LLC. 2014-2015. ALL RIGHTS RESERVED.
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 *
 * Copyright (C) Computer Institute, NUDT, China, 2018-2020. ALL RIGHTS RESERVED.
 */

#include <ucs/arch/cpu.h>
#include <ucs/sys/string.h>
#include "glex_iface.h"
#include "glex_ep.h"
#include "glex_channel.h"

static ucs_config_field_t uct_glex_iface_config_table[] = {
    /* This tuning controls the allocation priorities for SRQ */
    /* XXX, TODO, max_bcopy is srq_unit_size, should be moved to MD config parameters */
    { "", "ALLOC=huge,mmap,heap", NULL,
    ucs_offsetof(uct_glex_iface_config_t, super), UCS_CONFIG_TYPE_TABLE(uct_iface_config_table)},

    UCT_IFACE_MPOOL_CONFIG_FIELDS("SRQ_", -1, 0, "srq",
                                  ucs_offsetof(uct_glex_iface_config_t, mpool),
                                  "\nAttention: Setting this param with value != -1 is a dangerous thing\n"
                                  "and could cause deadlock or performance degradation."),

    {NULL}
};

ucs_status_t uct_glex_iface_event_arm(uct_iface_h tl_iface, unsigned events)
{
    uct_glex_iface_t *iface = ucs_derived_of(tl_iface, uct_glex_iface_t);

    if ((events & UCT_EVENT_SEND_COMP) &&
        !ucs_arbiter_is_empty(&iface->arbiter)) {
        /* cannot go to sleep, need to progress pending operations */
        return UCS_ERR_BUSY;
    }

    return UCS_OK;
}

ucs_status_t uct_glex_iface_flush(uct_iface_h tl_iface, unsigned flags,
		                  uct_completion_t *comp)
{
    uct_glex_iface_t *iface = ucs_derived_of(tl_iface, uct_glex_iface_t);

    if (comp != NULL) {
        return UCS_ERR_UNSUPPORTED;
    }

    if (0 == iface->outstanding) {
        UCT_TL_IFACE_STAT_FLUSH(ucs_derived_of(tl_iface, uct_base_iface_t));
        return UCS_OK;
    }

    UCT_TL_IFACE_STAT_FLUSH_WAIT(ucs_derived_of(tl_iface, uct_base_iface_t));
    return UCS_INPROGRESS;
}

ucs_status_t
uct_glex_iface_get_address(uct_iface_h tl_iface, uct_iface_addr_t *addr)
{
    uct_glex_iface_t *iface = ucs_derived_of(tl_iface, uct_glex_iface_t);
    glex_ep_addr_t *iface_addr = (glex_ep_addr_t *)addr;

    /* XXX, TODO, embed ep->key in iface_addr which can be sent to peer */
    iface_addr->v = iface->address.v;
    return UCS_OK;
}

int
uct_glex_iface_is_reachable(uct_iface_h tl_iface,
                            const uct_device_addr_t *dev_addr,
                            const uct_iface_addr_t *iface_addr)
{
    /* XXX, TODO, use md->config parameter, and dev_addr to decide if NIC can be reached.
     * return (dev_addr == iface->dev_id);
     */
    return 1;
}

static ucs_status_t
uct_glex_iface_query(uct_iface_h tl_iface,
                     uct_iface_attr_t *iface_attr)
{
    uct_glex_iface_t *iface = ucs_derived_of(tl_iface, uct_glex_iface_t);

    uct_base_iface_query(&iface->super, iface_attr);

    iface_attr->cap.put.max_short       = 0;	/* thus _put_short will not be called */
    iface_attr->cap.put.max_bcopy       = iface->glex_md->config.rma_buf_size;
    iface_attr->cap.put.min_zcopy       = 0;
    iface_attr->cap.put.max_zcopy       = iface->glex_md->dev_attr.max_rdma_data_len;
    iface_attr->cap.put.opt_zcopy_align = 1;
    iface_attr->cap.put.align_mtu       = iface_attr->cap.put.opt_zcopy_align;
    iface_attr->cap.put.max_iov         = 1;

    iface_attr->cap.get.max_bcopy       = iface->glex_md->config.rma_buf_size;
    iface_attr->cap.get.min_zcopy       = 0;
    iface_attr->cap.get.max_zcopy       = iface_attr->cap.put.max_zcopy;
    iface_attr->cap.get.opt_zcopy_align = 1;
    iface_attr->cap.get.align_mtu       = iface_attr->cap.get.opt_zcopy_align;
    iface_attr->cap.get.max_iov         = 1;

    iface_attr->cap.am.max_short       = UCT_GLEX_MP_DATA_LEN
                                                - sizeof(uct_glex_mp_hdr_t);
    /* XXX, TODO, if we use vc->er.recvq_stat, erq_unit_hdr_t is not needed */
    iface_attr->cap.am.max_bcopy       = iface->glex_md->config.srq_unit_size
                                                - sizeof(uct_glex_erq_unit_hdr_t);
    iface_attr->cap.am.min_zcopy       = 0;
    iface_attr->cap.am.max_zcopy       = 0;
    iface_attr->cap.am.opt_zcopy_align = 1;
    iface_attr->cap.am.align_mtu       = iface_attr->cap.am.opt_zcopy_align;
    iface_attr->cap.am.max_iov         = 1;

    iface_attr->device_addr_len        = 0;
    iface_attr->iface_addr_len         = sizeof(glex_ep_addr_t);
    iface_attr->ep_addr_len            = 0;
    iface_attr->max_conn_priv          = 0;
    iface_attr->cap.flags              = UCT_IFACE_FLAG_AM_SHORT         |
                                         UCT_IFACE_FLAG_AM_BCOPY         |
                                         UCT_IFACE_FLAG_PUT_SHORT        |
                                         UCT_IFACE_FLAG_PUT_BCOPY        |
                                         UCT_IFACE_FLAG_PUT_ZCOPY        |
                                         UCT_IFACE_FLAG_GET_BCOPY        |
                                         UCT_IFACE_FLAG_GET_ZCOPY        |
                                         UCT_IFACE_FLAG_PENDING          |
                                         UCT_IFACE_FLAG_CONNECT_TO_IFACE |
                                         UCT_IFACE_FLAG_CB_SYNC;

    // iface_attr->cap.event_flags        = UCT_IFACE_FLAG_EVENT_SEND_COMP |
    //                                      UCT_IFACE_FLAG_EVENT_RECV      |
    //                                      UCT_IFACE_FLAG_EVENT_ASYNC_CB;



    iface_attr->overhead               = 80e-9; /* 80 ns, XXX, TODO */
    iface_attr->bandwidth.dedicated    = 0;
    iface_attr->bandwidth.shared       = 12000.0 * UCS_MBYTE;
    iface_attr->latency                = ucs_linear_func_make(900e-9, 0); /* 900 ns */
    iface_attr->priority               = 0;

    return UCS_OK;
}

static void
uct_glex_srq_desc_key_init(uct_iface_h tl_iface, void *obj, uct_mem_h memh)
{
    uct_glex_srq_desc_t *desc = (uct_glex_srq_desc_t *)obj;
    uct_glex_key_t *key = memh;

    /* for SRQ unit, the off starts from data base */
    desc->base.mh.v = key->mh.v;
    desc->base.off  = (uintptr_t)(desc + 1) - (uintptr_t)key->address;
}

static void
uct_glex_erq_desc_key_init(uct_iface_h tl_iface, void *obj, uct_mem_h memh)
{
    uct_glex_erq_desc_t *desc = (uct_glex_erq_desc_t *)obj;
    uct_glex_key_t *key = memh;

    /* for ERQ, the off starts from desc base */
    desc->mh.v = key->mh.v;
    desc->off  = (uint32_t)((uintptr_t)desc - (uintptr_t)key->address);
}

static void
uct_glex_rma_desc_key_init(uct_iface_h tl_iface, void *obj, uct_mem_h memh)
{
    uct_glex_rma_desc_t *desc = (uct_glex_rma_desc_t *)obj;
    uct_glex_key_t *key = memh;

    /* for RMA buffer, the off starts from data base */
    desc->base.mh.v = key->mh.v;
    desc->base.off  = (uintptr_t)(desc + 1) - (uintptr_t)key->address;
}

static unsigned
uct_glex_progress(uct_iface_h tl_iface)
{
    uct_glex_iface_t *iface = ucs_derived_of(tl_iface, uct_glex_iface_t);
    unsigned count;

    /*
     *XXX, TODO, poll error desc:
     *  add a error member in iface struct, it will be set
     *  when error is detected in every _progress func?
     */
 
    count = uct_glex_probe_event(iface);
    if (!ucs_queue_is_empty(&iface->er.recv_vc_queue)) {
        count += uct_glex_er_progress(iface);
    }
    count += uct_glex_sr_progress(iface);
    uct_glex_probe_mp(iface);
    if (!ucs_queue_is_empty(&iface->sr.mp_vc_queue)) {
        count += uct_glex_sr_mp_progress(iface);
    }
    uct_glex_credit_refill(iface);

    ucs_arbiter_dispatch(&iface->arbiter, 1, uct_glex_ep_process_pending, NULL);

    return count;
}

static ucs_status_t
uct_glex_query_tl_devices(uct_md_h md,
                          uct_tl_device_resource_t **tl_devices_p,
                          unsigned *num_tl_devices_p)
{
    uct_glex_md_t *glex_md = ucs_derived_of(md, uct_glex_md_t);
    uct_tl_device_resource_t *tl_device;

    tl_device = ucs_calloc(1, sizeof(*tl_device), "device resource");
    if (NULL == tl_device) {
        ucs_error("Failed to allocate device resource");
        return UCS_ERR_NO_MEMORY;
    }

    ucs_snprintf_zero(tl_device->name, sizeof(tl_device->name), "%s%d",
                      UCT_GLEX_NI_PREFIX, glex_md->dev_id);
    tl_device->type       = UCT_DEVICE_TYPE_NET;
    tl_device->sys_device = UCS_SYS_DEVICE_ID_UNKNOWN;

    *num_tl_devices_p = 1;
    *tl_devices_p     = tl_device;
    return UCS_OK;
}

static UCS_CLASS_DECLARE_DELETE_FUNC(uct_glex_iface_t, uct_iface_t);

static uct_iface_ops_t uct_glex_iface_ops = {
    .ep_put_short             = uct_glex_ep_put_short,
    .ep_put_bcopy             = uct_glex_ep_put_bcopy,
    .ep_put_zcopy             = uct_glex_ep_put_zcopy,
    .ep_get_bcopy             = uct_glex_ep_get_bcopy,
    .ep_get_zcopy             = uct_glex_ep_get_zcopy,
    .ep_am_short              = uct_glex_ep_am_short,
    .ep_am_short_iov          = uct_base_ep_am_short_iov,
    .ep_am_bcopy              = uct_glex_ep_am_bcopy,
    .ep_pending_add           = uct_glex_ep_pending_add,
    .ep_pending_purge         = uct_glex_ep_pending_purge,
    .ep_flush                 = uct_glex_ep_flush,
    .ep_fence                 = uct_base_ep_fence,
    .ep_create                = UCS_CLASS_NEW_FUNC_NAME(uct_glex_ep_t),
    .ep_destroy               = UCS_CLASS_DELETE_FUNC_NAME(uct_glex_ep_t),
    .iface_fence              = uct_base_iface_fence,
    .iface_progress_enable    = uct_base_iface_progress_enable,
    .iface_progress_disable   = uct_base_iface_progress_disable,
    .iface_progress           = (void *)uct_glex_progress,
    // .iface_event_fd_get       = ucs_empty_function_return_unsupported,
    // .iface_event_arm          = uct_glex_iface_event_arm,
    .iface_flush              = uct_glex_iface_flush,
    .iface_close              = UCS_CLASS_DELETE_FUNC_NAME(uct_glex_iface_t),
    .iface_query              = uct_glex_iface_query,
    .iface_get_device_address = (void *)ucs_empty_function_return_success,
    .iface_get_address        = uct_glex_iface_get_address,
    .iface_is_reachable       = uct_glex_iface_is_reachable
};

static ucs_mpool_ops_t uct_glex_mpool_ops = {
    .chunk_alloc        = ucs_mpool_chunk_malloc,
    .chunk_release      = ucs_mpool_chunk_free,
    .obj_init           = NULL,
    .obj_cleanup        = NULL
};

static UCS_CLASS_INIT_FUNC(uct_glex_iface_t, uct_md_h md, uct_worker_h worker,
                           const uct_iface_params_t *params,
                           const uct_iface_config_t *tl_config)
{
    uct_glex_md_t *glex_md = ucs_derived_of(md, uct_glex_md_t);
    uct_glex_iface_config_t *config = ucs_derived_of(tl_config, uct_glex_iface_config_t);
    /* XXX, TODO, GLEX ATT alloc mode is 2^n, thus one SRQ unit is reserved for aligning elements */
    unsigned grow = (config->mpool.bufs_grow == 0) ? 510 : config->mpool.bufs_grow;
    ucs_status_t status;

    ucs_assert(params->open_mode & UCT_IFACE_OPEN_MODE_DEVICE);

    UCS_CLASS_CALL_SUPER_INIT(uct_base_iface_t, &uct_glex_iface_ops, &uct_base_iface_internal_ops,
                              md, worker, params, tl_config UCS_STATS_ARG(params->stats_root)
                              UCS_STATS_ARG(UCT_GLEX_TL_NAME)); //XXX, TODO

    self->glex_md = glex_md;
    self->glex_ep = glex_md->ep;
    glex_get_ep_addr(glex_md->ep, &self->address);
    self->outstanding = 0;
    self->key = glex_md->ep_attr.key;
    ucs_queue_head_init(&self->vc_tp_queue);
    ucs_arbiter_init(&self->arbiter);
    self->hash_key = uct_glex_vc_hash_key_init(self->address, 0);
    kh_init_inplace(uct_glex_vc, &self->vc_hash);
    //XXX, TODO self->rx_headroom

    status = ucs_mpool_init(&self->flush_pool,
                            0,
                            sizeof(uct_glex_flush_group_t),
                            0,
                            UCS_SYS_CACHE_LINE_SIZE,
                            128, 			//XXX, TODO
                            config->mpool.max_bufs,     //XXX, TODO
                            &uct_glex_mpool_ops,
                            "GLEX-DESC-FLUSH");
    if (UCS_OK != status) {
        ucs_error("FLUSH mpool creation failed");
        goto err_out;
    }

    status = uct_iface_mpool_init(&self->super,
                                  &self->sr.free_srq_desc,
                                  sizeof(uct_glex_srq_desc_t) + glex_md->config.srq_unit_size,
                                  sizeof(uct_glex_srq_desc_t), /* alignment offset */
                                  UCS_SYS_CACHE_LINE_SIZE,
                                  &config->mpool,
                                  grow,
                                  uct_glex_srq_desc_key_init,
                                  "GLEX-DESC-SRQ");
    if (UCS_OK != status) {
        ucs_error("SRQ mpool creation failed");
        goto clean_flush;
    }

    status = uct_iface_mpool_init(&self->super,
                                  &self->er.erq_pool,
                                  sizeof(uct_glex_erq_desc_t) + glex_md->config.erq_size,
                                  sizeof(uct_glex_erq_desc_t),
                                  UCS_SYS_CACHE_LINE_SIZE,
                                  &config->mpool,
                                  UCT_GLEX_ERQ_MPOOL_GROW,
                                  uct_glex_erq_desc_key_init,
                                  "GLEX-DESC-ERQ");
    if (UCS_OK != status) {
        ucs_error("ERQ mpool creation failed");
        goto clean_srq;
    }

    status = ucs_mpool_init(&self->rma.free_zc_desc,
                            0,
                            sizeof(uct_glex_zc_desc_t),
                            0,
                            1,
                            128,
                            UINT_MAX, /* XXX, TODO, md->config.zc_req_capacity */
                            &uct_glex_mpool_ops,
                            "GLEX-DESC-ZC");
    if (UCS_OK != status) {
        ucs_error("ZC mpool creation failed");
        goto clean_erq;
    }

    status = uct_iface_mpool_init(&self->super,
                                  &self->rma.free_buf_desc,
                                  sizeof(uct_glex_rma_desc_t) + glex_md->config.rma_buf_size,
                                  sizeof(uct_glex_rma_desc_t),
                                  UCS_SYS_CACHE_LINE_SIZE,
                                  &config->mpool,
                                  grow, /* XXX, TODO */
                                  uct_glex_rma_desc_key_init,
                                  "GLEX-DESC-RMA");
    if (UCS_OK != status) {
        ucs_error("RMA mpool creation failed");
        goto clean_zc;
    }

    status = ucs_mpool_init(&self->mp_info,
                            0,
                            sizeof(uct_glex_mp_info_t),
                            0,
                            1,
                            ucs_min(512, glex_md->ep_attr.mpq_capacity),
                            glex_md->ep_attr.mpq_capacity,
                            &uct_glex_mpool_ops,
                            "GLEX-MP-INFO");
    if (UCS_OK != status) {
        ucs_error("mp_info mpool creation failed");
        goto clean_rma;
    }

    status = uct_glex_channel_init(self);
    if (UCS_OK != status) {
        ucs_error("Could not init glex channel");
        goto clean_mp_info;
    }

    return UCS_OK;

clean_mp_info:
    ucs_mpool_cleanup(&self->mp_info, 1);
clean_rma:
    ucs_mpool_cleanup(&self->rma.free_buf_desc, 1);
clean_zc:
    ucs_mpool_cleanup(&self->rma.free_zc_desc, 1);
clean_erq:
    ucs_mpool_cleanup(&self->er.erq_pool, 1);
clean_srq:
    ucs_mpool_cleanup(&self->sr.free_srq_desc, 1);
clean_flush:
    ucs_mpool_cleanup(&self->flush_pool, 1);
err_out:
    ucs_error("Failed to activate interface");
    return status;
}

static void
uct_glex_release_queued_vc(uct_glex_iface_t *iface)
{
    uct_glex_vc_t *vc;

    while (!ucs_queue_is_empty(&iface->vc_tp_queue)) {
        vc = ucs_queue_pull_elem_non_empty(&iface->vc_tp_queue,
                                           uct_glex_vc_t, tp_queue);
        uct_glex_vc_release(iface, vc);
    }
}

static UCS_CLASS_CLEANUP_FUNC(uct_glex_iface_t)
{
    uct_base_iface_progress_disable(&self->super.super,
                                    UCT_PROGRESS_SEND | UCT_PROGRESS_RECV);
    uct_glex_channel_cleanup(self);
    uct_glex_release_queued_vc(self);
    kh_destroy_inplace(uct_glex_vc, &self->vc_hash);
    ucs_mpool_cleanup(&self->mp_info, 1);
    ucs_mpool_cleanup(&self->rma.free_buf_desc, 1);
    ucs_mpool_cleanup(&self->rma.free_zc_desc, 1);
    ucs_mpool_cleanup(&self->sr.free_srq_desc, 1);
    ucs_mpool_cleanup(&self->er.erq_pool, 1);
    ucs_arbiter_cleanup(&self->arbiter);
    ucs_mpool_cleanup(&self->flush_pool, 1);
}

UCS_CLASS_DEFINE(uct_glex_iface_t, uct_base_iface_t);

static UCS_CLASS_DEFINE_NEW_FUNC(uct_glex_iface_t, uct_iface_t, uct_md_h,
                                 uct_worker_h, const uct_iface_params_t*,
                                 const uct_iface_config_t *);
static UCS_CLASS_DEFINE_DELETE_FUNC(uct_glex_iface_t, uct_iface_t);

UCT_TL_DEFINE(&uct_glex_component,
              glex,
              uct_glex_query_tl_devices,
              uct_glex_iface_t,
              "GLEX_",
              uct_glex_iface_config_table,
              uct_glex_iface_config_t);
