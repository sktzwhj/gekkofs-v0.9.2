/**
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 * Copyright (c) UT-Battelle, LLC. 2014-2015. ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 *
 * Copyright (C) Computer Institute, NUDT, China, 2018-2020. ALL RIGHTS RESERVED.
 */

#include "glex_md.h"
#include "glex_def.h"

#include <ucs/arch/cpu.h>
#include <ucs/debug/log.h>
#include <ucs/sys/sys.h>
#include <ucs/sys/string.h>
#include <ucs/sys/math.h>
#include <ucm/api/ucm.h>
#include <ucs/vfs/base/vfs_obj.h>
#ifdef HAVE_PMIX
#include <pmix.h>
#endif
#ifdef HAVE_PMI
#include <pmi.h>
#endif

#define UCT_GLEX_MD_MEM_DEREG_CHECK_PARAMS(_params) \
    UCT_MD_MEM_DEREG_CHECK_PARAMS(_params, 1)

/**
 * Static information about glex job
 *
 * The information is static and does not change since job launch.
 * Therefore, the information is only fetched once.
 */
typedef struct uct_glex_job_info {
    int         pmi_num_of_ranks;
    int         pmi_rank_id;    /* XXX, TODO */
    uint32_t    key;
    int         initialized;
} uct_glex_job_info_t;

enum {
    UCT_GLEX_JOB_INFO_PMIX = 1,
    UCT_GLEX_JOB_INFO_PMI,
    UCT_GLEX_JOB_INFO_SLURM,
    UCT_GLEX_JOB_INFO_DEFAULT
};

static uct_glex_job_info_t job_info = {
    .initialized = 0,
};

static ucs_config_field_t uct_glex_md_config_table[] = {
    {"", "", NULL,
    ucs_offsetof(uct_glex_md_config_t, super), UCS_CONFIG_TYPE_TABLE(uct_md_config_table)},

    {"RCACHE", "try", "Enable using memory registration cache",
    ucs_offsetof(uct_glex_md_config_t, rcache_enable), UCS_CONFIG_TYPE_TERNARY},

    {"", "", NULL,
    ucs_offsetof(uct_glex_md_config_t, rcache),
    UCS_CONFIG_TYPE_TABLE(uct_md_config_rcache_table)},

    {"EP_TYPE", "1", "Type of glex endpoint, 0: PIO | 1: DMA",
    ucs_offsetof(uct_glex_md_config_t, channel.ep_type), UCS_CONFIG_TYPE_INT},

    {"EP_HC_MPQ", "0", "Use high capacity MPQ in glex endpoint",
    ucs_offsetof(uct_glex_md_config_t, channel.ep_hc_mpq), UCS_CONFIG_TYPE_BOOL},

    {"EP_HC_EQ", "0", "Use high capacity EQ in glex endpoint",
    ucs_offsetof(uct_glex_md_config_t, channel.ep_hc_eq), UCS_CONFIG_TYPE_BOOL},

    {"EP_HC_MR", "0", "Use high capacity memory register table",
    ucs_offsetof(uct_glex_md_config_t, channel.ep_hc_mr), UCS_CONFIG_TYPE_BOOL},

    {"EP_DQ_CAPACITY", "0", "DQ capacity of glex endpoint",
    ucs_offsetof(uct_glex_md_config_t, channel.ep_dq_capacity), UCS_CONFIG_TYPE_UINT},

    {"EP_MPQ_CAPACITY", "0", "MPQ capacity of glex endpoint",
    ucs_offsetof(uct_glex_md_config_t, channel.ep_mpq_capacity), UCS_CONFIG_TYPE_UINT},

    {"EP_EQ_CAPACITY", "0", "EQ capacity of glex endpoint",
    ucs_offsetof(uct_glex_md_config_t, channel.ep_eq_capacity), UCS_CONFIG_TYPE_UINT},

    {"EP_MPQ_POOL_UNITS", "0", "Credit Pool units in MPQ of glex endpoint",
    ucs_offsetof(uct_glex_md_config_t, channel.ep_mpq_pool_units), UCS_CONFIG_TYPE_UINT},

    {"SRQ_CAPACITY", UCT_GLEX_SRQ_CAPACITY, "Capacity of SRQ in SR channel",
    ucs_offsetof(uct_glex_md_config_t, channel.srq_capacity), UCS_CONFIG_TYPE_UINT},

    {"SRQ_UNIT_SIZE", UCT_GLEX_SRQ_UNIT_SIZE, "Unit size of SRQ in SR channel",
    ucs_offsetof(uct_glex_md_config_t, channel.srq_unit_size), UCS_CONFIG_TYPE_MEMUNITS},

    {"ER_MAX_CHANNELS", UCT_GLEX_ER_MAX_CHANNELS, "Maximum number of ER channels",
    ucs_offsetof(uct_glex_md_config_t, channel.er_max_channels), UCS_CONFIG_TYPE_UINT},

    {"ERQ_UNIT_SIZE", UCT_GLEX_ERQ_UNIT_SIZE, "Unit size of ERQ in each ER channel",
    ucs_offsetof(uct_glex_md_config_t, channel.erq_unit_size), UCS_CONFIG_TYPE_MEMUNITS},

    {"ERQ_SIZE", UCT_GLEX_ERQ_SIZE, "Size of ERQ in each ER channel",
    ucs_offsetof(uct_glex_md_config_t, channel.erq_size), UCS_CONFIG_TYPE_MEMUNITS},

    {"RMA_BUF_SIZE", UCT_GLEX_RMA_BUF_SIZE, "Size of RMA buffer unit",
    ucs_offsetof(uct_glex_md_config_t, channel.rma_buf_size), UCS_CONFIG_TYPE_MEMUNITS},

    {"RMA_REQ_CAPACITY", UCT_GLEX_RMA_REQ_CAPACITY, "Maximum number of pending RMA requests",
    ucs_offsetof(uct_glex_md_config_t, channel.rma_req_capacity), UCS_CONFIG_TYPE_UINT},

    {"ZC_REQ_CAPACITY", UCT_GLEX_ZC_REQ_CAPACITY, "Maximum number of pending ZC requests",
    ucs_offsetof(uct_glex_md_config_t, channel.zc_req_capacity), UCS_CONFIG_TYPE_UINT},

    {"SR_CREDIT_START", UCT_GLEX_SR_CREDIT_START, "Start credit of each VC in SR channel",
    ucs_offsetof(uct_glex_md_config_t, channel.sr_credit_start), UCS_CONFIG_TYPE_UINT},

    {"SR_CREDIT_INC", UCT_GLEX_SR_CREDIT_INC, "Credit increasing value of each VC in SR channel",
    ucs_offsetof(uct_glex_md_config_t, channel.sr_credit_inc), UCS_CONFIG_TYPE_UINT},

    {"SR_CREDIT_MAX", UCT_GLEX_SR_CREDIT_MAX, "Maximum credit of each VC in SR channel",
    ucs_offsetof(uct_glex_md_config_t, channel.sr_credit_max), UCS_CONFIG_TYPE_UINT},

    {"SR_RDMA_PUT", "0", "Use RDMA PUT in SR channel",
    ucs_offsetof(uct_glex_md_config_t, channel.sr_rdma_put), UCS_CONFIG_TYPE_BOOL},

    {"NUM_PROCS", UCT_GLEX_NUM_PROCS_DEFAULT, "Default num of processes",
    ucs_offsetof(uct_glex_md_config_t, channel.num_procs), UCS_CONFIG_TYPE_UINT},

    {NULL}
};

/* get NPROCS info from SLURM env vars */
static int
uct_glex_job_info_slurm_init(void)
{
    char *env_val;

    if ((NULL == (env_val = getenv("SLURM_STEP_NUM_TASKS")))
        && (NULL == (env_val = getenv("SLURM_NTASKS")))) {
        return 0;       /* process is not started by SLURM */
    }

    job_info.initialized = UCT_GLEX_JOB_INFO_SLURM;
    return (int)strtol(env_val, NULL, 0);
}

#if defined(HAVE_PMIX)
static pmix_proc_t my_proc;

static ucs_status_t
uct_glex_job_info_pmi_init(void)
{
    pmix_status_t rc;
    pmix_proc_t proc;
    pmix_value_t *val;

    rc = PMIx_Init(&my_proc, NULL, 0);
    if (PMIX_SUCCESS != rc) {
        ucs_error("PMIx_Init failed, error status: %d", rc);
        return UCS_ERR_IO_ERROR;
    }

    PMIX_LOAD_NSPACE(proc.nspace, my_proc.nspace);
    proc.rank = PMIX_RANK_WILDCARD;

    rc = PMIx_Get(&proc, PMIX_JOB_SIZE, NULL, 0, &val);
    if (PMIX_SUCCESS != rc) {
        ucs_error("PMIx_Get JOB_SIZE failed, error status: %d", rc);
        return UCS_ERR_IO_ERROR;
    }
    job_info.pmi_num_of_ranks = val->data.uint32;
    PMIX_VALUE_RELEASE(val);
    ucs_debug("PMIx job size: %d", job_info.pmi_num_of_ranks);

    job_info.initialized = UCT_GLEX_JOB_INFO_PMIX;
    return UCS_OK;
}

#elif defined(HAVE_PMI)
static ucs_status_t
uct_glex_job_info_pmi_init(void)
{
    int spawned = 0;
    int rc;

    rc = PMI_Init(&spawned);
    if (PMI_SUCCESS != rc) {
        ucs_error("PMI_Init failed, error status: %d", rc);
        return UCS_ERR_IO_ERROR;
    }
    ucs_debug("PMI spawned %d", spawned);

    rc = PMI_Get_size(&job_info.pmi_num_of_ranks);
    if (PMI_SUCCESS != rc) {
        ucs_error("PMI_Get_size failed, error status: %d", rc);
        return UCS_ERR_IO_ERROR;
    }
    ucs_debug("PMI size %d", job_info.pmi_num_of_ranks);

    rc = PMI_Get_rank(&job_info.pmi_rank_id);
    if (PMI_SUCCESS != rc) {
        ucs_error("PMI_Get_rank failed, error status: %d", rc);
        return UCS_ERR_IO_ERROR;
    }
    ucs_debug("PMI rank %d", job_info.pmi_rank_id);

    job_info.initialized = UCT_GLEX_JOB_INFO_PMI;
    return UCS_OK;
}

#else
static ucs_status_t
uct_glex_job_info_pmi_init(void)
{
    return ucs_empty_function_return_unsupported();
}
#endif

static void
uct_glex_job_info_pmi_fini(void)
{
#ifdef HAVE_PMIX
    if (job_info.initialized == UCT_GLEX_JOB_INFO_PMIX) {
        PMIx_Finalize(NULL, 0);
    }
#endif
#ifdef HAVE_PMI
    if (job_info.initialized == UCT_GLEX_JOB_INFO_PMI) {
        PMI_Finalize();
    }
#endif
}

static ucs_status_t
uct_glex_get_job_info(const uct_glex_md_config_t *md_config)
{
    char *env_val;
    unsigned int slurm_job_id;

    if (job_info.initialized) {
        return UCS_OK;
    }

    job_info.pmi_num_of_ranks = uct_glex_job_info_slurm_init();
    if (0 == job_info.pmi_num_of_ranks) {
        /* try to get NPROCS using PMI/PMIx API */
        if (UCS_OK != uct_glex_job_info_pmi_init()) {
            job_info.pmi_num_of_ranks = md_config->channel.num_procs;
            job_info.initialized = UCT_GLEX_JOB_INFO_DEFAULT;
            //ucs_warn("PMI query failure, assume %d ranks", job_info.pmi_num_of_ranks);
        }
    }

    if (UCT_GLEX_JOB_INFO_SLURM == job_info.initialized) {
        env_val = getenv("SLURM_JOB_ID");
        if (NULL == env_val) {
            env_val = getenv("SLURM_JOBID");
        }

        slurm_job_id = (unsigned int)strtol(env_val, NULL, 0);
        job_info.key = GLEX_EP_MAKE_KEY(slurm_job_id);
        if (0 == job_info.key) {
            job_info.key = GLEX_EP_MAKE_KEY(UCT_GLEX_KEY_DEFAULT);
        }
    } else {
        /* job is not submitted by SLURM, use default key */
        job_info.key = GLEX_EP_MAKE_KEY(UCT_GLEX_KEY_DEFAULT);
    }

    ucs_debug("GLEX job info was activated");
    return UCS_OK;
}

ucs_status_t
uct_glex_md_query(uct_md_h uct_md, uct_md_attr_t *md_attr)
{
    uct_glex_md_t *md = ucs_derived_of(uct_md, uct_glex_md_t);

    md_attr->cap.flags            = UCT_MD_FLAG_REG       |
                                    UCT_MD_FLAG_NEED_MEMH |
                                    UCT_MD_FLAG_NEED_RKEY |
                                    UCT_MD_FLAG_INVALIDATE;
    md_attr->cap.reg_mem_types    = UCS_BIT(UCS_MEMORY_TYPE_HOST);
    md_attr->cap.alloc_mem_types  = 0;
    md_attr->cap.access_mem_types = UCS_BIT(UCS_MEMORY_TYPE_HOST);
    md_attr->cap.detect_mem_types = 0;
    md_attr->cap.max_alloc        = 0;
    md_attr->cap.max_reg          = ULONG_MAX;
    md_attr->rkey_packed_size     = sizeof(uct_glex_key_t);
    md_attr->reg_cost             = md->reg_cost;

    memset(&md_attr->local_cpus, 0xff, sizeof(md_attr->local_cpus));
    return UCS_OK;
}

static void
uct_glex_make_md_name(char md_name[UCT_MD_NAME_MAX], unsigned dev_id)
{
    ucs_snprintf_zero(md_name, UCT_MD_NAME_MAX, "%s/%s%d",
                      UCT_GLEX_MD_PREFIX, UCT_GLEX_NI_PREFIX, dev_id);
}

static ucs_status_t
uct_glex_query_md_resources(uct_component_t *component,
                            uct_md_resource_desc_t **resources_p,
		            unsigned *num_resources_p)
{
    uct_md_resource_desc_t *resources;
    uint32_t num_of_devices;
    unsigned int i;

    glex_num_of_device(&num_of_devices);
    if (0 == num_of_devices) {
        ucs_debug("Failed to find glex device");
        return uct_md_query_empty_md_resource(resources_p, num_resources_p);
    }

    resources = ucs_calloc(num_of_devices, sizeof(*resources), "glex resources");
    if (NULL == resources) {
        return UCS_ERR_NO_MEMORY;
    }

    for (i = 0; i < num_of_devices; i++) {
        uct_glex_make_md_name(resources[i].md_name, i);
    }

    *resources_p     = resources;
    *num_resources_p = num_of_devices;
    return UCS_OK;
}

static void
uct_glex_md_close(uct_md_h md)
{
    uct_glex_md_t *glex_md = ucs_derived_of(md, uct_glex_md_t);

    if (glex_md->rcache != NULL) {
        ucs_rcache_destroy(glex_md->rcache);
    }
    glex_destroy_ep(glex_md->ep);
    glex_close_device(glex_md->dev);
    ucs_free(glex_md);
    uct_glex_job_info_pmi_fini();
}

static ucs_status_t
uct_glex_mem_reg_internal(uct_md_h md, void *address, size_t length,
                          unsigned flags, unsigned silent,
                          uct_glex_key_t *key)
{
    uct_glex_md_t *glex_md = ucs_derived_of(md, uct_glex_md_t);
    glex_mem_handle_t glex_mh;
    glex_ret_t glex_rc;

    glex_rc = glex_register_mem(glex_md->ep,
                                address, length,
                                GLEX_MEM_READ | GLEX_MEM_WRITE,
                                &glex_mh);
    if (GLEX_SUCCESS != glex_rc) {
        if (!silent) {
            /* do not report error in silent mode: it called from rcache
             * internals, rcache will try to register memory again with
             * more accurate data
             */
            ucs_error("GLEX create region failed: %s", glex_error_str(glex_rc));
        }
        return UCS_ERR_IO_ERROR;
    }

    key->mh.v    = glex_mh.v;
    key->address = (uintptr_t)address;

    return UCS_OK;
}

static ucs_status_t
uct_glex_mem_reg(uct_md_h md, void *address, size_t length,
                 unsigned flags, uct_mem_h *memh_p)
{
    uct_glex_key_t *key;
    ucs_status_t status;

    key = ucs_malloc(sizeof(uct_glex_key_t), "uct_glex_key_t");
    if (NULL == key) {
        ucs_error("Failed to allocate memory for uct_glex_key_t");
        return UCS_ERR_NO_MEMORY;
    }

    status = uct_glex_mem_reg_internal(md, address, length, flags, 0, key);
    if (UCS_OK == status) {
        *memh_p = key;
    } else {
        ucs_free(key);
    }
    return status;
}

static ucs_status_t
uct_glex_mem_dereg_internal(uct_md_h md, uct_glex_key_t *key)
{
    uct_glex_md_t *glex_md = ucs_derived_of(md, uct_glex_md_t);
    glex_ret_t glex_rc;

    ucs_assert_always(glex_md->ep != NULL);
    ucs_assert_always(key->address != 0);

    glex_rc = glex_deregister_mem(glex_md->ep, key->mh);
    if (GLEX_SUCCESS != glex_rc) {
        ucs_error("GLEX destroy region failed, err = %s",
                  glex_error_str(glex_rc));
        return UCS_ERR_IO_ERROR;
    }

    return UCS_OK;
}

static ucs_status_t
uct_glex_mem_dereg(uct_md_h md,
                   const uct_md_mem_dereg_params_t *params)
{
    uct_glex_key_t *key;
    ucs_status_t status;

    UCT_GLEX_MD_MEM_DEREG_CHECK_PARAMS(params);

    key = (uct_glex_key_t *)params->memh;
    status = uct_glex_mem_dereg_internal(md, key);
    if (status != UCS_OK) {
        ucs_warn("failed to dregister memory");
    }

    ucs_free(key);
    if (UCT_MD_MEM_DEREG_FIELD_VALUE(params, flags, FIELD_FLAGS, 0) &
        UCT_MD_MEM_DEREG_FLAG_INVALIDATE) {
        ucs_assert(params->comp != NULL); /* suppress coverity false-positive */
        uct_invoke_completion(params->comp, UCS_OK);
    }

    return status;
}

static ucs_status_t
uct_glex_rkey_pack(uct_md_h md, uct_mem_h memh,
                   void *rkey_buffer)
{
    uct_glex_key_t *packed = (uct_glex_key_t *)rkey_buffer;
    uct_glex_key_t *key = (uct_glex_key_t *)memh;

    packed->mh.v    = key->mh.v;
    packed->address = key->address;
    ucs_trace("packed rkey: mh 0x%"PRIx64" address %"PRIxPTR,
              key->mh.v, key->address);
    return UCS_OK;
}

static ucs_status_t
uct_glex_rkey_unpack(uct_component_t *component,
                     const void *rkey_buffer, uct_rkey_t *rkey_p,
                     void **handle_p)
{
    uct_glex_key_t *packed = (uct_glex_key_t *)rkey_buffer;
    uct_glex_key_t *key;

    key = ucs_malloc(sizeof(uct_glex_key_t), "uct_glex_key_t");
    if (NULL == key) {
        ucs_error("Failed to allocate memory for uct_glex_key_t");
        return UCS_ERR_NO_MEMORY;
    }

    key->mh.v    = packed->mh.v;
    key->address = packed->address;
    *handle_p = NULL;
    *rkey_p = (uintptr_t)key;
    ucs_trace("unpacked rkey: key %p mh 0x%"PRIx64" address %"PRIxPTR,
              key, key->mh.v, key->address);
    return UCS_OK;
}

static ucs_status_t
uct_glex_rkey_release(uct_component_t *component,
                      uct_rkey_t rkey, void *handle)
{
    ucs_assert(NULL == handle);
    ucs_free((void *)rkey);
    return UCS_OK;
}

static uct_md_ops_t md_ops = {
    .close              = uct_glex_md_close,
    .query              = uct_glex_md_query,
    .mem_alloc          = (void *)ucs_empty_function,
    .mem_free           = (void *)ucs_empty_function,
    .mem_reg            = uct_glex_mem_reg,
    .mem_dereg          = uct_glex_mem_dereg,
    .mkey_pack          = uct_glex_rkey_pack,
    .detect_memory_type = ucs_empty_function_return_unsupported,
};

static inline uct_glex_rcache_region_t*
uct_glex_rcache_region_from_memh(uct_mem_h memh)
{
    return ucs_container_of(memh, uct_glex_rcache_region_t, key);
}

static ucs_status_t
uct_glex_mem_rcache_reg(uct_md_h uct_md, void *address,
                        size_t length, unsigned flags,
                        uct_mem_h *memh_p)
{
    uct_glex_md_t *md = ucs_derived_of(uct_md, uct_glex_md_t);
    ucs_rcache_region_t *rregion;
    ucs_status_t status;

    status = ucs_rcache_get(md->rcache, address, length, PROT_READ|PROT_WRITE,
                            &flags, &rregion);
    if (status != UCS_OK) {
        return status;
    }

    ucs_assert(rregion->refcount > 0);
    *memh_p = &ucs_derived_of(rregion, uct_glex_rcache_region_t)->key;
    return UCS_OK;
}

static void
uct_glex_mem_region_invalidate_cb(void *arg)
{
    uct_completion_t *comp = arg;

    uct_invoke_completion(comp, UCS_OK);
}

static ucs_status_t
uct_glex_mem_rcache_dereg(uct_md_h uct_md,
                          const uct_md_mem_dereg_params_t *params)
{
    uct_glex_md_t *md = ucs_derived_of(uct_md, uct_glex_md_t);
    uct_glex_rcache_region_t *region;

    UCT_GLEX_MD_MEM_DEREG_CHECK_PARAMS(params);

    region = uct_glex_rcache_region_from_memh(params->memh);
    if (UCT_MD_MEM_DEREG_FIELD_VALUE(params, flags, FIELD_FLAGS, 0) &
        UCT_MD_MEM_DEREG_FLAG_INVALIDATE) {
        ucs_rcache_region_invalidate(md->rcache, &region->super,
                                     uct_glex_mem_region_invalidate_cb,
                                     params->comp);
    }

    ucs_rcache_region_put(md->rcache, &region->super);
    return UCS_OK;
}

static uct_md_ops_t uct_glex_md_rcache_ops = {
    .close                  = uct_glex_md_close,
    .query                  = uct_glex_md_query,
    .mkey_pack              = uct_glex_rkey_pack,
    .mem_reg                = uct_glex_mem_rcache_reg,
    .mem_dereg              = uct_glex_mem_rcache_dereg,
    .is_sockaddr_accessible = ucs_empty_function_return_zero_int,
    .detect_memory_type     = ucs_empty_function_return_unsupported,
};

static ucs_status_t
uct_glex_rcache_mem_reg_cb(void *context, ucs_rcache_t *rcache,
                           void *arg, ucs_rcache_region_t *rregion,
                           uint16_t rcache_mem_reg_flags)
{
    uct_glex_rcache_region_t *region = ucs_derived_of(rregion, uct_glex_rcache_region_t);
    uct_glex_md_t *md = context;
    int *flags = arg;

    return uct_glex_mem_reg_internal(&md->super, (void*)region->super.super.start,
                                     region->super.super.end - region->super.super.start,
                                     *flags,
                                     rcache_mem_reg_flags & UCS_RCACHE_MEM_REG_HIDE_ERRORS,
                                     &region->key);
}

static void
uct_glex_rcache_mem_dereg_cb(void *context, ucs_rcache_t *rcache,
                             ucs_rcache_region_t *rregion)
{
    uct_glex_md_t *md = context;
    uct_glex_rcache_region_t *region;

    region = ucs_derived_of(rregion, uct_glex_rcache_region_t);
    uct_glex_mem_dereg_internal(&md->super, &region->key);
}

static void
uct_glex_rcache_dump_region_cb(void *context, ucs_rcache_t *rcache,
                               ucs_rcache_region_t *rregion, char *buf,
                               size_t max)
{
    uct_glex_rcache_region_t *region = ucs_derived_of(rregion, uct_glex_rcache_region_t);
    uct_glex_key_t *key = &region->key;

    snprintf(buf, max, "mh 0x%"PRIx64" address %p", key->mh.v, (void*)key->address);
}

static ucs_rcache_ops_t uct_glex_rcache_ops = {
    .mem_reg     = uct_glex_rcache_mem_reg_cb,
    .mem_dereg   = uct_glex_rcache_mem_dereg_cb,
    .dump_region = uct_glex_rcache_dump_region_cb
};

static unsigned int
uct_glex_set_dq_capacity(int num_of_ranks)
{
    if (num_of_ranks <= 4096) {
        return 4096;
    } else {
        return 8192;
    }
}

static unsigned int
uct_glex_set_mpq_pool_units(int num_of_ranks)
{
    /* XXX, TODO, how to set pool_units */
    if (num_of_ranks < 1024) {
        return 4096;
    } else if (num_of_ranks < 4096) {
        return 8192;
    } else {
        return 16384;
    }
}

static void
uct_glex_set_ep_attr(uct_glex_md_t *glex_md,
		     const uct_glex_channel_config_t *ch_config)
{
    struct glex_ep_attr *ep_attr = &glex_md->ep_attr;
    unsigned int pool_units;

    if (ch_config->ep_type) {
        ep_attr->type = GLEX_EP_TYPE_NORMAL;
    } else {
        ep_attr->type = GLEX_EP_TYPE_FAST;
    }
    ep_attr->num = GLEX_ANY_EP_NUM;

    if (ch_config->ep_hc_mr) {
        ep_attr->key = GLEX_EP_MAKE_HC_UMMT_KEY(job_info.key);
    } else {
        ep_attr->key = job_info.key;
    }

    if (ch_config->ep_dq_capacity) {
        ep_attr->dq_capacity = ch_config->ep_dq_capacity;
    } else {
        ep_attr->dq_capacity = uct_glex_set_dq_capacity(job_info.pmi_num_of_ranks);
    }

    if (ch_config->ep_mpq_capacity) {
        ep_attr->mpq_capacity = ch_config->ep_mpq_capacity;
    } else {
        pool_units = uct_glex_set_mpq_pool_units(job_info.pmi_num_of_ranks);
        ep_attr->mpq_capacity = 2 * job_info.pmi_num_of_ranks
                                + ucs_max(pool_units, ch_config->ep_mpq_pool_units);
    }
    if (ch_config->ep_hc_mpq
            || (ep_attr->mpq_capacity > glex_md->dev_attr.max_ep_mpq_capacity)) {
        ep_attr->mpq_type = GLEX_MPQ_TYPE_HIGH_CAPACITY;
    } else {
        ep_attr->mpq_type = GLEX_MPQ_TYPE_NORMAL;
    }

    if (ch_config->ep_eq_capacity) {
        ep_attr->eq_capacity = ch_config->ep_eq_capacity;
    } else {
        ep_attr->eq_capacity = ch_config->srq_capacity
                                + (ch_config->er_max_channels
                                        * (ch_config->erq_size / ch_config->erq_unit_size))
                                + ch_config->rma_req_capacity
                                + ch_config->zc_req_capacity;
    }
    if (ch_config->ep_hc_eq
            || (ep_attr->eq_capacity > glex_md->dev_attr.max_ep_eq_capacity)) {
        ep_attr->eq_type = GLEX_EQ_TYPE_HIGH_CAPACITY;
    } else {
        ep_attr->eq_type = GLEX_EQ_TYPE_NORMAL;
    }
}

static ucs_status_t
uct_glex_check_config(uct_glex_md_t *glex_md,
                      const uct_glex_md_config_t *md_config)
{
    /* setting initial configuration, TODO change to iface config */
    glex_md->config.srq_unit_size    = md_config->channel.srq_unit_size;
    glex_md->config.srq_capacity     = md_config->channel.srq_capacity;
    glex_md->config.er_max_channels  = md_config->channel.er_max_channels;
    glex_md->config.erq_size         = md_config->channel.erq_size;
    glex_md->config.erq_unit_size    = md_config->channel.erq_unit_size;
    glex_md->config.rma_buf_size     = md_config->channel.rma_buf_size;
    glex_md->config.rma_req_capacity = md_config->channel.rma_req_capacity;
    glex_md->config.zc_req_capacity  = md_config->channel.zc_req_capacity;

    if ((0 == glex_md->config.srq_capacity)
            || (glex_md->config.erq_unit_size < UCT_GLEX_ERQ_UNIT_MIN_SIZE)
            || (glex_md->config.erq_unit_size > UCT_GLEX_ERQ_UNIT_MAX_SIZE)
            || (!ucs_is_pow2(glex_md->config.erq_unit_size))
            || (glex_md->config.erq_size < (glex_md->config.erq_unit_size * 8))
            || (glex_md->config.erq_size > UCT_GLEX_ERQ_MAX_SIZE)
            || (glex_md->config.srq_unit_size > UCT_GLEX_SRQ_UNIT_MAX_SIZE)
            || ((glex_md->config.srq_unit_size * glex_md->config.srq_capacity)
                        > UCT_GLEX_SRQ_MAX_SIZE)
            || (0 == glex_md->config.zc_req_capacity)
            || (glex_md->config.zc_req_capacity > glex_md->dev_attr.att_units)) {
        return UCS_ERR_INVALID_PARAM;
    }

    //XXX, TODO, check rma_buf_size, check mpq_pool_units

    return UCS_OK;
}

static ucs_status_t
uct_glex_check_fc_config(uct_glex_md_t *glex_md, 
                         const uct_glex_channel_config_t *ch_config)
{
    glex_md->config.sr_rdma_put     = ch_config->sr_rdma_put;
    glex_md->config.sr_credit_start = ch_config->sr_credit_start;
    glex_md->config.sr_credit_inc   = ch_config->sr_credit_inc;
    glex_md->config.sr_credit_max   = ch_config->sr_credit_max;

    glex_md->config.sr_credit_pool = glex_md->ep_attr.mpq_capacity
                                     - (ch_config->sr_credit_start + 1)
                                        * job_info.pmi_num_of_ranks;
    if ((glex_md->config.sr_credit_pool < 0)
            || (glex_md->config.sr_credit_start == 0)) {
        return UCS_ERR_INVALID_PARAM;
    }

    return UCS_OK;
}

static ucs_status_t
uct_glex_md_open(uct_component_t *component,
                 const char *md_name,
		 const uct_md_config_t *uct_md_config,
		 uct_md_h *md_p)
{
    static int count = 0;
    const uct_glex_md_config_t *md_config =
                        ucs_derived_of(uct_md_config, uct_glex_md_config_t);
    uct_glex_md_t *glex_md;
    ucs_rcache_params_t rcache_params;
    char tmp_md_name[UCT_MD_NAME_MAX];
    uint32_t num_of_devices;
    unsigned int i;
    ucs_status_t status;
    glex_ret_t glex_rc;
    //printf("uct_glex_md_open is called \n");

    status = uct_glex_get_job_info(md_config);
    if (UCS_OK != status) {
        return status;
    }

    glex_md = ucs_malloc(sizeof(uct_glex_md_t), "uct_glex_md_t");
    if (NULL == glex_md) {
        ucs_error("Failed to allocate memory for uct_glex_md_t");
        return UCS_ERR_NO_MEMORY;
    }

    glex_md->super.ops         = &md_ops;
    glex_md->super.component   = &uct_glex_component;
    glex_md->reg_cost          = ucs_linear_func_make(1200e-9, 0.007e-9); //XXX, TODO
    glex_md->rcache            = NULL;

    glex_num_of_device(&num_of_devices);
    if (0 == num_of_devices) {
        ucs_debug("Failed to find glex device");
        status = UCS_ERR_NO_DEVICE;
        goto clean_md;
    }

    for (i = 0; i < num_of_devices; i++) {
        uct_glex_make_md_name(tmp_md_name, i);
        if (!strcmp(tmp_md_name, md_name)) {
            break;
        }
    }
    if (i == num_of_devices) {
        ucs_debug("Unknown md_name: %s", md_name);
        status = UCS_ERR_NO_DEVICE;
        goto clean_md;
    }

    glex_md->dev_id = i;
    glex_rc = glex_open_device(i, &glex_md->dev);
    if (GLEX_SUCCESS != glex_rc) {
        ucs_error("Could not open glex device #%d", i);
        status = UCS_ERR_IO_ERROR;
        goto clean_md;
    }
    glex_query_device(glex_md->dev, &glex_md->dev_attr);

    status = uct_glex_check_config(glex_md, md_config);
    if (UCS_OK != status) {
        goto clean_dev;
    }

    uct_glex_set_ep_attr(glex_md, &md_config->channel);

    glex_rc = glex_create_ep(glex_md->dev, &glex_md->ep_attr, &glex_md->ep);
    if (GLEX_NO_EP_RESOURCE == glex_rc) {
        /* If the specified type of ep is full, try another type */
        if (GLEX_EP_TYPE_NORMAL == glex_md->ep_attr.type) {
            glex_md->ep_attr.type = GLEX_EP_TYPE_FAST;
        } else {
            glex_md->ep_attr.type = GLEX_EP_TYPE_NORMAL;
        }
        ucs_error("Cannot create the specified type of ep, try another type");
        glex_rc = glex_create_ep(glex_md->dev, &glex_md->ep_attr, &glex_md->ep);
    }
    if (GLEX_SUCCESS != glex_rc) {
         ucs_error("Could not create endpoint on glex device #%d", i);
         status = UCS_ERR_IO_ERROR;
         goto clean_dev;
    }

    status = uct_glex_check_fc_config(glex_md, &md_config->channel);
    if (UCS_OK != status) {
        goto clean_ep;
    }

    if (md_config->rcache_enable != UCS_NO) {
        uct_md_set_rcache_params(&rcache_params, &md_config->rcache);
        rcache_params.region_struct_size = sizeof(uct_glex_rcache_region_t);
        rcache_params.max_alignment      = ucs_get_page_size();
        rcache_params.ucm_events         = UCM_EVENT_VM_UNMAPPED;
        rcache_params.context            = glex_md;
        rcache_params.ops                = &uct_glex_rcache_ops;
        rcache_params.flags              = 0;
        status = ucs_rcache_create(&rcache_params, "glex rcache",
                                   ucs_stats_get_root(), &glex_md->rcache);
        if (status == UCS_OK) {
            glex_md->super.ops = &uct_glex_md_rcache_ops;
            glex_md->reg_cost  = ucs_linear_func_make(md_config->rcache.overhead, 0);
        } else {
            ucs_assert(glex_md->rcache == NULL);
            if (md_config->rcache_enable == UCS_YES) {
                ucs_error("Failed to create registration cache: %s",
                          ucs_status_string(status));
                uct_glex_md_close(&glex_md->super);
                goto err_out;
            } else {
                ucs_debug("Could not create registration cache: %s",
                          ucs_status_string(status));
            }
        }
    }

    *md_p = (uct_md_h)glex_md;
    return UCS_OK;

clean_ep:
    glex_destroy_ep(glex_md->ep);
clean_dev:
    glex_close_device(glex_md->dev);
clean_md:
    ucs_free(glex_md);
err_out:
    return status;
}

static void uct_glex_md_vfs_init(uct_md_h md)
{
    uct_glex_md_t *glex_md = (uct_glex_md_t *)md;

    if (glex_md->rcache != NULL) {
        ucs_vfs_obj_add_sym_link(md, glex_md->rcache, "rcache");
    }
}

uct_component_t uct_glex_component = {
    .query_md_resources = uct_glex_query_md_resources,
    .md_open            = uct_glex_md_open,
    .cm_open            = ucs_empty_function_return_unsupported,
    .rkey_unpack        = uct_glex_rkey_unpack,
    .rkey_ptr           = ucs_empty_function_return_unsupported,
    .rkey_release       = uct_glex_rkey_release,
    .name               = UCT_GLEX_MD_PREFIX,
    .md_config          = {
        .name           = "GLEX memory domain",
        .prefix         = "GLEX_",
        .table          = uct_glex_md_config_table,
        .size           = sizeof(uct_glex_md_config_t),
    },
    .cm_config          = UCS_CONFIG_EMPTY_GLOBAL_LIST_ENTRY,
    .tl_list            = UCT_COMPONENT_TL_LIST_INITIALIZER(&uct_glex_component),
    .flags              = 0,
    .md_vfs_init        = uct_glex_md_vfs_init
};
UCT_COMPONENT_REGISTER(&uct_glex_component);
