/*
 * Copyright (c) 2013-2021 UChicago Argonne, LLC and The HDF Group.
 * Copyright (c) 2014 UT-Battelle, LLC.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "na_plugin.h"

#include "mercury_list.h"
#include "mercury_time.h"

#include <cci.h>

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

/****************/
/* Local Macros */
/****************/

/* Error compat */
#define NA_INVALID_PARAM    NA_INVALID_ARG
#define NA_SIZE_ERROR       NA_MSGSIZE
#define NA_NOMEM_ERROR      NA_NOMEM
#define NA_PERMISSION_ERROR NA_PERMISSION
#define NA_ADDRINUSE_ERROR  NA_ADDRINUSE

/* Max tag */
#define NA_CCI_MAX_TAG ((1 << 30) - 1)

/************************************/
/* Local Type and Struct Definition */
/************************************/

typedef uint32_t cci_msg_tag_t;
typedef uint64_t cci_size_t;
typedef uintptr_t cci_op_id_t;
typedef struct na_cci_addr na_cci_addr_t;
typedef struct na_cci_op_id na_cci_op_id_t;
typedef struct na_cci_mem_handle na_cci_mem_handle_t;
typedef struct na_cci_class na_cci_class_t;

#define NA_CCI_CLASS(na_class)                                                 \
    ((struct na_cci_class *) (na_class->plugin_class))

/* na_cci_addr */
struct na_cci_addr {
    cci_connection_t *cci_addr;      /* CCI addr */
    HG_QUEUE_HEAD(na_cci_op_id) rxs; /* Posted recvs */
    HG_QUEUE_HEAD(na_cci_info_recv_expected)
    early;                    /* Expected recvs not yet posted */
    char *uri;                /* Peer's URI */
    hg_atomic_int32_t refcnt; /* Reference counter */
    na_bool_t unexpected;     /* Address generated from unexpected recv */
    na_bool_t self;           /* Boolean for self */
    HG_LIST_ENTRY(na_cci_addr) entry;
};

struct na_cci_mem_handle {
    cci_rma_handle_t h;
    na_ptr_t base;   /* Initial address of memory */
    na_size_t size;  /* Size of memory */
    na_uint8_t attr; /* Flag of operation access */
};

typedef enum na_cci_rma_op {
    NA_CCI_RMA_PUT, /* Request a put operation */
    NA_CCI_RMA_GET  /* Request a get operation */
} na_cci_rma_op_t;

struct na_cci_info_send_unexpected {
    cci_op_id_t op_id; /* CCI operation ID */
};

struct na_cci_info_recv_unexpected {
    void *buf;
    cci_size_t buf_size;
    cci_size_t actual_size;
    na_cci_addr_t *na_cci_addr;
    cci_msg_tag_t tag;
    HG_QUEUE_ENTRY(na_cci_info_recv_unexpected) entry;
};

struct na_cci_info_send_expected {
    cci_op_id_t op_id; /* CCI operation ID */
};

struct na_cci_info_recv_expected {
    na_cci_addr_t *na_cci_addr;
    HG_QUEUE_ENTRY(na_cci_info_recv_expected)
    entry;
    cci_op_id_t op_id; /* CCI operation ID */
    void *buf;
    cci_size_t buf_size;
    cci_size_t actual_size;
    cci_msg_tag_t tag;
};

struct na_cci_info_put {
    cci_op_id_t request_op_id;
    cci_op_id_t transfer_op_id;
    na_bool_t transfer_completed;
    cci_size_t transfer_actual_size;
    cci_op_id_t completion_op_id;
    cci_size_t completion_actual_size;
    na_bool_t internal_progress;
    cci_connection_t *remote_addr;
};

struct na_cci_info_get {
    cci_op_id_t request_op_id;
    cci_op_id_t transfer_op_id;
    cci_size_t transfer_actual_size;
    na_bool_t internal_progress;
    cci_connection_t *remote_addr;
};

/* na_cci_op_id  TODO uint64_t cookie for cancel ? */
struct na_cci_op_id {
    HG_QUEUE_ENTRY(na_cci_op_id) entry;
    na_context_t *context;
    na_cb_type_t type;
    na_cb_t callback; /* Callback */
    void *arg;
    hg_atomic_int32_t completed; /* Operation completed */
    hg_atomic_int32_t canceled;  /* Operation canceled  */
    union {
        struct na_cci_info_send_unexpected send_unexpected;
        struct na_cci_info_recv_unexpected recv_unexpected;
        struct na_cci_info_send_expected send_expected;
        struct na_cci_info_recv_expected recv_expected;
        struct na_cci_info_put put;
        struct na_cci_info_get get;
    } info;
    struct na_cb_completion_data completion_data;
};

struct na_cci_class {
    cci_endpoint_t *endpoint;
    HG_QUEUE_HEAD(na_cci_op_id) early;       /* Unexpected rxs not yet posted */
    hg_thread_mutex_t test_unexpected_mutex; /* Mutex */
    HG_QUEUE_HEAD(na_cci_info_recv_unexpected)
    unexpected_msg_queue; /* Posted unexpected message queue */
    hg_thread_mutex_t unexpected_msg_queue_mutex;    /* Mutex */
    HG_QUEUE_HEAD(na_cci_op_id) unexpected_op_queue; /* Unexpected op queue */
    hg_thread_mutex_t unexpected_op_queue_mutex;     /* Mutex */
    HG_LIST_HEAD(na_cci_addr)
    accept_conn_list;                         /* List of accepted connections */
    hg_thread_mutex_t accept_conn_list_mutex; /* Mutex */
    char *uri;
    int fd;
};

typedef union cci_msg {
    struct msg_size {
        uint32_t expect : 1;
        uint32_t bye : 1;
        uint32_t tag : 30;
    } size;

    struct msg_send {
        uint32_t expect : 1;
        uint32_t bye : 1;
        uint32_t tag : 30;
        char data[1];
    } send;

    uint32_t net;
} cci_msg_t;

/********************/
/* Local Prototypes */
/********************/

/* check_protocol */
static na_bool_t
na_cci_check_protocol(const char *protocol_name);

/* initialize */
static na_return_t
na_cci_initialize(
    na_class_t *na_class, const struct na_info *na_info, na_bool_t listen);

/**
 * initialize
 *
 * \param method_list [IN]      (Optional) list of available methods depend on
 *                              CCI configuration, e.g., tcp, verbs, gni, sm,
 * ... \param listen_addr [IN]      (Optional) e.g., CCI URI
 */
static na_return_t
na_cci_init(na_class_t *na_class);

/* finalize */
static na_return_t
na_cci_finalize(na_class_t *na_class);

/* op_create */
static na_op_id_t *
na_cci_op_create(na_class_t *na_class);

/* op_destroy */
static na_return_t
na_cci_op_destroy(na_class_t *na_class, na_op_id_t *op_id);

/* addr_lookup */
static na_return_t
na_cci_addr_lookup(na_class_t *na_class, const char *name, na_addr_t *addr);

/* addr_self */
static na_return_t
na_cci_addr_self(na_class_t *na_class, na_addr_t *addr);

static void
addr_decref(na_cci_addr_t *na_cci_addr);

/* addr_dup */
static na_return_t
na_cci_addr_dup(na_class_t *na_class, na_addr_t addr, na_addr_t *new_addr);

/* addr_free */
static na_return_t
na_cci_addr_free(na_class_t *na_class, na_addr_t addr);

/* addr_is_self */
static na_bool_t
na_cci_addr_is_self(na_class_t *na_class, na_addr_t addr);

/* addr_to_string */
static na_return_t
na_cci_addr_to_string(
    na_class_t *na_class, char *buf, na_size_t *buf_size, na_addr_t addr);

/* msg_get_max */
static na_size_t
na_cci_msg_get_max_unexpected_size(const na_class_t *na_class);

static na_size_t
na_cci_msg_get_max_expected_size(const na_class_t *na_class);

static na_tag_t
na_cci_msg_get_max_tag(const na_class_t *na_class);

/* msg_send_unexpected */
static na_return_t
na_cci_msg_send_unexpected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, const void *buf, na_size_t buf_size,
    void *plugin_data, na_addr_t dest_addr, na_uint8_t dest_id, na_tag_t tag,
    na_op_id_t *op_id);

/* msg_recv_unexpected */
static na_return_t
na_cci_msg_recv_unexpected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, void *buf, na_size_t buf_size,
    void *plugin_data, na_op_id_t *op_id);

/* msg_send_expected */
static na_return_t
na_cci_msg_send_expected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, const void *buf, na_size_t buf_size,
    void *plugin_data, na_addr_t dest_addr, na_uint8_t dest_id, na_tag_t tag,
    na_op_id_t *op_id);

/* msg_recv_expected */
static na_return_t
na_cci_msg_recv_expected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, void *buf, na_size_t buf_size,
    void *plugin_data, na_addr_t source_addr, na_uint8_t source_id,
    na_tag_t tag, na_op_id_t *op_id);

static na_return_t
na_cci_msg_unexpected_push(
    na_class_t *na_class, struct na_cci_info_recv_unexpected *rx);

static struct na_cci_info_recv_unexpected *
na_cci_msg_unexpected_pop(na_class_t *na_class);

static na_return_t
na_cci_msg_unexpected_op_push(
    na_class_t *na_class, struct na_cci_op_id *na_cci_op_id);

static struct na_cci_op_id *
na_cci_msg_unexpected_op_pop(na_class_t *na_class);

/* mem_handle */
static na_return_t
na_cci_mem_handle_create(na_class_t *na_class, void *buf, na_size_t buf_size,
    unsigned long flags, na_mem_handle_t *mem_handle);

static na_return_t
na_cci_mem_handle_free(na_class_t *na_class, na_mem_handle_t mem_handle);

static na_return_t
na_cci_mem_register(na_class_t *na_class, na_mem_handle_t mem_handle);

static na_return_t
na_cci_mem_deregister(na_class_t *na_class, na_mem_handle_t mem_handle);

/* mem_handle serialization */
static na_size_t
na_cci_mem_handle_get_serialize_size(
    na_class_t *na_class, na_mem_handle_t mem_handle);

static na_return_t
na_cci_mem_handle_serialize(na_class_t *na_class, void *buf, na_size_t buf_size,
    na_mem_handle_t mem_handle);

static na_return_t
na_cci_mem_handle_deserialize(na_class_t *na_class, na_mem_handle_t *mem_handle,
    const void *buf, na_size_t buf_size);

/* put */
static na_return_t
na_cci_put(na_class_t *na_class, na_context_t *context, na_cb_t callback,
    void *arg, na_mem_handle_t local_mem_handle, na_offset_t local_offset,
    na_mem_handle_t remote_mem_handle, na_offset_t remote_offset,
    na_size_t length, na_addr_t remote_addr, na_uint8_t remote_id,
    na_op_id_t *op_id);

/* get */
static na_return_t
na_cci_get(na_class_t *na_class, na_context_t *context, na_cb_t callback,
    void *arg, na_mem_handle_t local_mem_handle, na_offset_t local_offset,
    na_mem_handle_t remote_mem_handle, na_offset_t remote_offset,
    na_size_t length, na_addr_t remote_addr, na_uint8_t remote_id,
    na_op_id_t *op_id);

/* poll_get_fd */
static int
na_cci_poll_get_fd(na_class_t *na_class, na_context_t *context);

/* progress */
static na_return_t
na_cci_progress(
    na_class_t *na_class, na_context_t *context, unsigned int timeout);

static na_return_t
na_cci_complete(na_cci_addr_t *na_cci_addr, struct na_cci_op_id *na_cci_op_id,
    na_return_t ret);

static void
na_cci_release(void *arg);

/* cancel */
static na_return_t
na_cci_cancel(na_class_t *na_class, na_context_t *context, na_op_id_t *op_id);

/*******************/
/* Local Variables */
/*******************/

const struct na_class_ops NA_PLUGIN_OPS(cci) = {
    "cci",                                /* name */
    na_cci_check_protocol,                /* check_protocol */
    na_cci_initialize,                    /* initialize */
    na_cci_finalize,                      /* finalize */
    NULL,                                 /* cleanup */
    NULL,                                 /* context_create */
    NULL,                                 /* context_destroy */
    na_cci_op_create,                     /* op_create */
    na_cci_op_destroy,                    /* op_destroy */
    na_cci_addr_lookup,                   /* addr_lookup */
    na_cci_addr_free,                     /* addr_free */
    NULL,                                 /* addr_set_remove */
    na_cci_addr_self,                     /* addr_self */
    na_cci_addr_dup,                      /* addr_dup */
    NULL,                                 /* addr_cmp */
    na_cci_addr_is_self,                  /* addr_is_self */
    na_cci_addr_to_string,                /* addr_to_string */
    NULL,                                 /* addr_get_serialize_size */
    NULL,                                 /* addr_serialize */
    NULL,                                 /* addr_deserialize */
    na_cci_msg_get_max_unexpected_size,   /* msg_get_max_unexpected_size */
    na_cci_msg_get_max_expected_size,     /* msg_get_max_expected_size */
    NULL,                                 /* msg_get_unexpected_header_size */
    NULL,                                 /* msg_get_expected_header_size */
    na_cci_msg_get_max_tag,               /* msg_get_max_tag */
    NULL,                                 /* msg_buf_alloc */
    NULL,                                 /* msg_buf_free */
    NULL,                                 /* msg_init_unexpected */
    na_cci_msg_send_unexpected,           /* msg_send_unexpected */
    na_cci_msg_recv_unexpected,           /* msg_recv_unexpected */
    NULL,                                 /* msg_init_expected */
    na_cci_msg_send_expected,             /* msg_send_expected */
    na_cci_msg_recv_expected,             /* msg_recv_expected */
    na_cci_mem_handle_create,             /* mem_handle_create */
    NULL,                                 /* mem_handle_create_segment */
    na_cci_mem_handle_free,               /* mem_handle_free */
    NULL,                                 /* mem_handle_get_max_segments */
    na_cci_mem_register,                  /* mem_register */
    na_cci_mem_deregister,                /* mem_deregister */
    na_cci_mem_handle_get_serialize_size, /* mem_handle_get_serialize_size */
    na_cci_mem_handle_serialize,          /* mem_handle_serialize */
    na_cci_mem_handle_deserialize,        /* mem_handle_deserialize */
    na_cci_put,                           /* put */
    na_cci_get,                           /* get */
    na_cci_poll_get_fd,                   /* poll_get_fd */
    NULL,                                 /* poll_try_wait */
    na_cci_progress,                      /* progress */
    na_cci_cancel                         /* cancel */
};

/********************/
/* Plugin callbacks */
/********************/

/*---------------------------------------------------------------------------*/
static na_bool_t
na_cci_check_protocol(const char *protocol_name)
{
    na_bool_t accept = NA_FALSE;
    int ret = 0, i = 0;
    uint32_t caps = 0;
    cci_device_t *const *devices, *device = NULL;

    /*
     * init CCI, get_devices, and check if a device on this transport
     * exists and is up
     */

    /* Initialize CCI */
    ret = cci_init(CCI_ABI_VERSION, 0, &caps);
    if (ret) {
        NA_LOG_ERROR("cci_init() failed with %s", cci_strerror(NULL, ret));
        goto out;
    }
    /* Get the available devices */
    ret = cci_get_devices(&devices);
    if (ret) {
        NA_LOG_ERROR(
            "cci_get_devices() failed with %s", cci_strerror(NULL, ret));
        goto out;
    }
    for (i = 0;; i++) {
        device = devices[i];

        if (!device)
            break;

        if (!strcmp(device->transport, protocol_name)) {
            if (!device->up) {
                NA_LOG_ERROR("device %s (transport %s) is down", device->name,
                    device->transport);
                continue;
            }
            break;
        }
    }

    if (!device) {
        NA_LOG_ERROR("requested transport %s is not available", protocol_name);
        goto out;
    }
    if (device)
        accept = NA_TRUE;

    /* Finalize CCI */
    ret = cci_finalize();
    if (ret) {
        NA_LOG_ERROR("CCI_finalize() failed with %s", cci_strerror(NULL, ret));
        goto out;
    }

out:
    return accept;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_check_interface(const char *hostname, char **device_name)
{
    struct ifaddrs *ifaddrs, *ifaddr;
    na_return_t ret = NA_SUCCESS;

    if (getifaddrs(&ifaddrs) == -1) {
        NA_LOG_ERROR("getifaddrs() failed");
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

    /* Check and compare interfaces */
    for (ifaddr = ifaddrs; ifaddr != NULL; ifaddr = ifaddr->ifa_next) {
        char host[NI_MAXHOST];
        char ip[INET_ADDRSTRLEN];

        if (ifaddr->ifa_addr == NULL)
            continue;

        if (ifaddr->ifa_addr->sa_family != AF_INET)
            continue;

        /* Get hostname */
        if (getnameinfo(ifaddr->ifa_addr, sizeof(struct sockaddr_in), host,
                NI_MAXHOST, NULL, 0, 0) != 0) {
            NA_LOG_ERROR(
                "Name could not be resolved for: %s", ifaddr->ifa_name);
            ret = NA_PROTOCOL_ERROR;
            goto out;
        }

        /* Get IP */
        if (!inet_ntop(ifaddr->ifa_addr->sa_family,
                &((struct sockaddr_in *) ifaddr->ifa_addr)->sin_addr, ip,
                INET_ADDRSTRLEN)) {
            NA_LOG_ERROR("IP could not be resolved for: %s", ifaddr->ifa_name);
            ret = NA_PROTOCOL_ERROR;
            goto out;
        }

        /* Compare IP and hostname */
        if (!strcmp(host, hostname) || !strcmp(ip, hostname)) {
            *device_name = strdup(ifaddr->ifa_name);
            break;
        }
    }

out:
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_initialize(
    na_class_t *na_class, const struct na_info *na_info, na_bool_t listen)
{
    int rc = 0, i = 0;
    uint32_t caps = 0;
    cci_device_t *const *devices = NULL, *device = NULL;
    cci_endpoint_t *endpoint = NULL;
    char *uri = NULL;
    na_return_t ret = NA_SUCCESS;
    int fd = -1, *fd_p = &fd;
    char *device_name = NULL;
    char *hostname = NULL;
    char *service = na_info->host_name;
    na_bool_t device_found = NA_FALSE;

    (void) listen;

    /* Initialize CCI */
    rc = cci_init(CCI_ABI_VERSION, 0, &caps);
    if (rc) {
        NA_LOG_ERROR("cci_init() failed with %s", cci_strerror(NULL, rc));
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

    /* Get the available devices */
    rc = cci_get_devices(&devices);
    if (rc) {
        NA_LOG_ERROR(
            "cci_get_devices() failed with %s", cci_strerror(NULL, rc));
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

    /* Get hostname/port info if available (tcp or verbs) */
    if ((((strcmp(na_info->protocol_name, "tcp") == 0) ||
            strcmp(na_info->protocol_name, "verbs") == 0)) &&
        na_info->host_name && strstr(na_info->host_name, ":")) {
        hostname = strdup(na_info->host_name);
        if (!hostname) {
            NA_LOG_ERROR("Could not duplicate string");
            ret = NA_NOMEM_ERROR;
            goto out;
        }

        /* Extract hostname */
        strtok_r(hostname, ":", &service);

        /* Somehow loopback interface is not supported */
        if (strcmp("localhost", hostname) && strcmp("127.0.0.1", hostname)) {
            ret = na_cci_check_interface(hostname, &device_name);
            if (ret != NA_SUCCESS) {
                NA_LOG_ERROR("Could not check interfaces");
                goto out;
            }

            /* Allow for passing device name directly */
            if (!device_name)
                device_name = strdup(hostname);
        }
    }

    /* Check devices */
    for (i = 0;; i++) {
        device = devices[i];

        if (!device)
            break;

        if (!strcmp(device->transport, na_info->protocol_name)) {
            if (!device->up) {
                NA_LOG_WARNING("device %s transport %s is down", device->name,
                    device->transport);
                continue;
            }
            if (device_name && strcmp(device->name, device_name))
                continue; /* did not match */

            device_found = NA_TRUE;
            break;
        }
    }
    if (!device_found) {
        NA_LOG_ERROR("Could not find requested device");
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

    na_class->plugin_class = malloc(sizeof(struct na_cci_class));
    if (!na_class->plugin_class) {
        NA_LOG_ERROR("Could not allocate NA private data class");
        ret = NA_NOMEM_ERROR;
        goto out;
    }
    memset(na_class->plugin_class, 0, sizeof(struct na_cci_class));
    if (na_info->na_init_info &&
        (na_info->na_init_info->progress_mode & NA_NO_BLOCK))
        fd_p = NULL;

    /* Create unspecified endpoint if service is set */
    if (service)
        rc = cci_create_endpoint_at(device, service, 0, &endpoint, fd_p);
    else
        rc = cci_create_endpoint(device, 0, &endpoint, fd_p);
    if (rc) {
        NA_LOG_ERROR(
            "cci_create_endpoint() failed with %s", cci_strerror(NULL, rc));
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }
    NA_CCI_CLASS(na_class)->endpoint = endpoint;
    NA_CCI_CLASS(na_class)->fd = fd;

    rc = cci_get_opt(endpoint, CCI_OPT_ENDPT_URI, &uri);
    if (rc) {
        NA_LOG_ERROR(
            "cci_get_opt(URI) failed with %s", cci_strerror(endpoint, rc));
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

    NA_CCI_CLASS(na_class)->uri = strdup(uri);
    free(uri);

    ret = na_cci_init(na_class);

out:
    free(hostname);
    free(device_name);
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_init(na_class_t *na_class)
{
    na_return_t ret = NA_SUCCESS;

    HG_QUEUE_INIT(&NA_CCI_CLASS(na_class)->unexpected_msg_queue);
    HG_QUEUE_INIT(&NA_CCI_CLASS(na_class)->unexpected_op_queue);
    HG_LIST_INIT(&NA_CCI_CLASS(na_class)->accept_conn_list);

    /* Initialize mutex/cond */
    hg_thread_mutex_init(&NA_CCI_CLASS(na_class)->test_unexpected_mutex);
    hg_thread_mutex_init(&NA_CCI_CLASS(na_class)->unexpected_msg_queue_mutex);
    hg_thread_mutex_init(&NA_CCI_CLASS(na_class)->unexpected_op_queue_mutex);
    hg_thread_mutex_init(&NA_CCI_CLASS(na_class)->accept_conn_list_mutex);

    if (ret != NA_SUCCESS) {
        na_cci_finalize(na_class);
    }
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_finalize(na_class_t *na_class)
{
    na_cci_class_t *priv = na_class->plugin_class;
    na_return_t ret = NA_SUCCESS;
    int rc;

    free(priv->uri);

    /* Free connections */
    while (!HG_LIST_IS_EMPTY(&priv->accept_conn_list)) {
        na_cci_addr_t *na_cci_addr = HG_LIST_FIRST(&priv->accept_conn_list);
        HG_LIST_REMOVE(na_cci_addr, entry);

        addr_decref(na_cci_addr);
    }

    /* Check that unexpected op queue is empty */
    if (!HG_QUEUE_IS_EMPTY(&priv->unexpected_op_queue)) {
        NA_LOG_ERROR("Unexpected op queue should be empty");
        ret = NA_PROTOCOL_ERROR;
    }

    /* Check that unexpected message queue is empty */
    if (!HG_QUEUE_IS_EMPTY(&priv->unexpected_msg_queue)) {
        NA_LOG_ERROR("Unexpected msg queue should be empty");
        ret = NA_PROTOCOL_ERROR;
    }

    rc = cci_destroy_endpoint(priv->endpoint);
    if (rc) {
        NA_LOG_ERROR(
            "cci_destroy_endpoint() failed with %s", cci_strerror(NULL, rc));
        ret = NA_PROTOCOL_ERROR;
    }

    /* Finalize CCI */
    rc = cci_finalize();
    if (rc) {
        NA_LOG_ERROR("CCI_finalize() failed with %s", cci_strerror(NULL, rc));
        ret = NA_PROTOCOL_ERROR;
    }
    /* Destroy mutex/cond */
    hg_thread_mutex_destroy(&priv->test_unexpected_mutex);
    hg_thread_mutex_destroy(&priv->unexpected_msg_queue_mutex);
    hg_thread_mutex_destroy(&priv->unexpected_op_queue_mutex);
    hg_thread_mutex_destroy(&priv->accept_conn_list_mutex);

    free(na_class->plugin_class);

    return ret;
}

/*---------------------------------------------------------------------------*/
static na_op_id_t *
na_cci_op_create(na_class_t NA_UNUSED *na_class)
{
    na_cci_op_id_t *na_cci_op_id = NULL;

    na_cci_op_id = (na_cci_op_id_t *) malloc(sizeof(na_cci_op_id_t));
    if (!na_cci_op_id) {
        NA_LOG_ERROR("Could not allocate NA CCI operation ID");
        goto done;
    }
    memset(na_cci_op_id, 0, sizeof(na_cci_op_id_t));

    /* Completed by default */
    hg_atomic_set32(&na_cci_op_id->completed, 1);

done:
    return (na_op_id_t *) na_cci_op_id;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_op_destroy(na_class_t NA_UNUSED *na_class, na_op_id_t *op_id)
{
    na_cci_op_id_t *na_cci_op_id = (na_cci_op_id_t *) op_id;
    na_return_t ret = NA_SUCCESS;

    /* No more references, cleanup */
    free(na_cci_op_id);

    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_addr_lookup(na_class_t *na_class, const char *name, na_addr_t *addr)
{
    cci_endpoint_t *e = NA_CCI_CLASS(na_class)->endpoint;
    char *uri = NA_CCI_CLASS(na_class)->uri;
    na_cci_addr_t *na_cci_addr = NULL;
    na_return_t ret = NA_SUCCESS;
    int rc;

    /* Allocate addr */
    na_cci_addr = (na_cci_addr_t *) malloc(sizeof(*na_cci_addr));
    if (!na_cci_addr) {
        NA_LOG_ERROR("Could not allocate CCI addr");
        ret = NA_NOMEM_ERROR;
        goto out;
    }
    na_cci_addr->cci_addr = NULL;
    HG_QUEUE_INIT(&na_cci_addr->rxs);
    HG_QUEUE_INIT(&na_cci_addr->early);
    na_cci_addr->uri = strdup(name);
    /* one for the lookup callback and one for the caller to hold until
     * addr_free(). na_cci_complete will decref for the lookup callback.
     */
    hg_atomic_set32(&na_cci_addr->refcnt, 1);
    na_cci_addr->unexpected = NA_FALSE;
    na_cci_addr->self = NA_FALSE;

    /* TODO we would need to ensure that connect completes before using the addr
     */
    rc = cci_connect(e, name, uri, (uint32_t) strlen(uri) + 1, CCI_CONN_ATTR_RO,
        na_cci_addr, 0, NULL);
    if (rc) {
        NA_LOG_ERROR(
            "cci_connect(%s) failed with %s", name, cci_strerror(e, rc));
        if (rc == CCI_ETIMEDOUT)
            ret = NA_TIMEOUT;
        else
            ret = NA_PROTOCOL_ERROR;
        goto out;
    }

    *addr = (na_addr_t) na_cci_addr;

out:
    if (ret != NA_SUCCESS) {
        free(na_cci_addr);
    }
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_addr_self(na_class_t *na_class, na_addr_t *addr)
{
    na_cci_addr_t *na_cci_addr = NULL;
    na_return_t ret = NA_SUCCESS;

    /* Allocate addr */
    na_cci_addr = (na_cci_addr_t *) malloc(sizeof(*na_cci_addr));
    if (!na_cci_addr) {
        NA_LOG_ERROR("Could not allocate CCI addr");
        ret = NA_NOMEM_ERROR;
        goto out;
    }
    na_cci_addr->cci_addr = 0;
    na_cci_addr->uri = strdup(NA_CCI_CLASS(na_class)->uri);
    na_cci_addr->unexpected = NA_FALSE;
    na_cci_addr->self = NA_TRUE;
    hg_atomic_set32(&na_cci_addr->refcnt, 1);

    *addr = (na_addr_t) na_cci_addr;

out:
    if (ret != NA_SUCCESS) {
        free(na_cci_addr);
    }
    return ret;
}

/*---------------------------------------------------------------------------*/
static void
addr_addref(na_cci_addr_t *na_cci_addr)
{
    assert(hg_atomic_get32(&na_cci_addr->refcnt));
    hg_atomic_incr32(&na_cci_addr->refcnt);
    return;
}

/*---------------------------------------------------------------------------*/
static void
addr_decref(na_cci_addr_t *na_cci_addr)
{
    cci_connection_t *c = na_cci_addr->cci_addr;

    assert(hg_atomic_get32(&na_cci_addr->refcnt) > 0);

    /* If there are more references, return */
    if (hg_atomic_decr32(&na_cci_addr->refcnt))
        return;

    /* No more references, cleanup */
    na_cci_addr->cci_addr = NULL;
    cci_disconnect(c);

    free(na_cci_addr->uri);
    free(na_cci_addr);

    return;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_addr_dup(
    na_class_t NA_UNUSED *na_class, na_addr_t addr, na_addr_t *new_addr)
{
    na_cci_addr_t *na_cci_addr = (na_cci_addr_t *) addr;
    addr_addref(na_cci_addr); /* for na_cci_addr_free() */
    *new_addr = addr;

    return NA_SUCCESS;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_addr_free(na_class_t NA_UNUSED *na_class, na_addr_t addr)
{
    na_cci_addr_t *na_cci_addr = (na_cci_addr_t *) addr;
    na_return_t ret = NA_SUCCESS;

    if (!na_cci_addr) {
        NA_LOG_ERROR("NULL CCI addr");
        ret = NA_INVALID_PARAM;
        return ret;
    }

    addr_decref(na_cci_addr);

    return ret;
}

/*---------------------------------------------------------------------------*/
static na_bool_t
na_cci_addr_is_self(na_class_t NA_UNUSED *na_class, na_addr_t addr)
{
    na_cci_addr_t *na_cci_addr = (na_cci_addr_t *) addr;

    return na_cci_addr->self;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_addr_to_string(na_class_t NA_UNUSED *na_class, char *buf,
    na_size_t *buf_size, na_addr_t addr)
{
    na_cci_addr_t *na_cci_addr = (na_cci_addr_t *) addr;
    na_size_t string_len;
    na_return_t ret = NA_SUCCESS;

    string_len = strlen(na_cci_addr->uri);
    if (buf) {
        if (string_len >= *buf_size) {
            NA_LOG_ERROR("Buffer size too small to copy addr");
            ret = NA_SIZE_ERROR;
        } else {
            strcpy(buf, na_cci_addr->uri);
        }
    }
    *buf_size = string_len + 1;

    return ret;
}

/*---------------------------------------------------------------------------*/
static na_size_t
na_cci_msg_get_max_unexpected_size(const na_class_t *na_class)
{
    cci_endpoint_t *e = NA_CCI_CLASS(na_class)->endpoint;
    cci_msg_t msg;
    na_size_t max_unexpected_size = e->device->max_send_size - sizeof(msg.size);

    return max_unexpected_size;
}

/*---------------------------------------------------------------------------*/
static na_size_t
na_cci_msg_get_max_expected_size(const na_class_t *na_class)
{
    cci_endpoint_t *e = NA_CCI_CLASS(na_class)->endpoint;
    cci_msg_t msg;
    na_size_t max_expected_size = e->device->max_send_size - sizeof(msg.size);

    return max_expected_size;
}

/*---------------------------------------------------------------------------*/
static na_tag_t
na_cci_msg_get_max_tag(const na_class_t NA_UNUSED *na_class)
{
    na_tag_t max_tag = NA_CCI_MAX_TAG;

    return max_tag;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_msg_send_unexpected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, const void *buf, na_size_t buf_size,
    void NA_UNUSED *plugin_data, na_addr_t dest_addr,
    na_uint8_t NA_UNUSED dest_id, na_tag_t tag, na_op_id_t *op_id)
{
    na_cci_addr_t *na_cci_addr = (na_cci_addr_t *) dest_addr;
    na_cci_op_id_t *na_cci_op_id = (na_cci_op_id_t *) op_id;
    na_return_t ret = NA_SUCCESS;
    int rc;
    cci_msg_t msg;
    struct iovec iov[2];

    addr_addref(na_cci_addr); /* for na_cci_complete() */

    if (!na_cci_addr->cci_addr) {
        NA_LOG_ERROR("not connected to peer %s", na_cci_addr->uri);
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

    /* Check op_id */
    NA_CHECK_ERROR(
        na_cci_op_id == NULL, out, ret, NA_INVALID_ARG, "Invalid operation ID");
    NA_CHECK_ERROR(!hg_atomic_get32(&na_cci_op_id->completed), out, ret,
        NA_BUSY, "Attempting to use OP ID that was not completed");

    na_cci_op_id->context = context;
    na_cci_op_id->type = NA_CB_SEND_UNEXPECTED;
    na_cci_op_id->callback = callback;
    na_cci_op_id->arg = arg;
    hg_atomic_set32(&na_cci_op_id->completed, 0);
    hg_atomic_set32(&na_cci_op_id->canceled, 0);
    na_cci_op_id->info.send_unexpected.op_id = 0;

    msg.send.expect = 0;
    msg.send.bye = 0;
    msg.send.tag = tag;

    iov[0].iov_base = &msg;
    iov[0].iov_len = sizeof(msg.size);
    iov[1].iov_base = (void *) buf;
    iov[1].iov_len = buf_size;

    /* Post the CCI unexpected send request */
    rc = cci_sendv(na_cci_addr->cci_addr, iov, 2, na_cci_op_id, 0);
    if (rc) {
        cci_endpoint_t *endpoint = NA_CCI_CLASS(na_class)->endpoint;
        NA_LOG_ERROR("cci_sendv() failed with %s", cci_strerror(endpoint, rc));
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

out:
    if (ret != NA_SUCCESS) {
        addr_decref(na_cci_addr);
        hg_atomic_set32(&na_cci_op_id->completed, 1);
    }
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_msg_recv_unexpected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, void *buf, na_size_t buf_size,
    void NA_UNUSED *plugin_data, na_op_id_t *op_id)
{
    na_cci_op_id_t *na_cci_op_id = (na_cci_op_id_t *) op_id;
    struct na_cci_info_recv_unexpected *rx = NULL;
    na_return_t ret = NA_SUCCESS;

    /* Check op_id */
    NA_CHECK_ERROR(
        na_cci_op_id == NULL, out, ret, NA_INVALID_ARG, "Invalid operation ID");
    NA_CHECK_ERROR(!hg_atomic_get32(&na_cci_op_id->completed), out, ret,
        NA_BUSY, "Attempting to use OP ID that was not completed");

    na_cci_op_id->context = context;
    na_cci_op_id->type = NA_CB_RECV_UNEXPECTED;
    na_cci_op_id->callback = callback;
    na_cci_op_id->arg = arg;
    hg_atomic_set32(&na_cci_op_id->completed, 0);
    hg_atomic_set32(&na_cci_op_id->canceled, 0);
    na_cci_op_id->info.recv_unexpected.buf = buf;
    na_cci_op_id->info.recv_unexpected.buf_size = (cci_size_t) buf_size;

    /* Look for an unexpected message already received */
    rx = na_cci_msg_unexpected_pop(na_class);

    if (rx) {
        na_size_t msg_len = rx->buf_size;

        if (na_cci_op_id->info.recv_unexpected.buf_size < msg_len)
            msg_len = na_cci_op_id->info.recv_unexpected.buf_size;
        memcpy(na_cci_op_id->info.recv_unexpected.buf, rx->buf, msg_len);
        na_cci_op_id->info.recv_unexpected.actual_size = msg_len;
        na_cci_op_id->info.recv_unexpected.na_cci_addr = rx->na_cci_addr;
        na_cci_op_id->info.recv_unexpected.tag = rx->tag;

        addr_addref(rx->na_cci_addr); /* for na_cci_complete() */
        ret = na_cci_complete(rx->na_cci_addr, na_cci_op_id, NA_SUCCESS);
        if (ret != NA_SUCCESS) {
            NA_LOG_ERROR("Could not complete operation");
            goto out;
        }
        free(rx->buf);
        free(rx);
    } else {
        /* Nothing has been received yet so add op_id to progress queue */
        ret = na_cci_msg_unexpected_op_push(na_class, na_cci_op_id);
        if (ret != NA_SUCCESS) {
            NA_LOG_ERROR("Could not push operation ID");
            goto out;
        }
    }

out:
    if (ret != NA_SUCCESS) {
        hg_atomic_set32(&na_cci_op_id->completed, 1);
    }
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_msg_unexpected_push(
    na_class_t *na_class, struct na_cci_info_recv_unexpected *rx)
{
    na_return_t ret = NA_SUCCESS;

    if (!rx) {
        NA_LOG_ERROR("NULL unexpected info");
        ret = NA_INVALID_PARAM;
        goto out;
    }
    hg_thread_mutex_lock(&NA_CCI_CLASS(na_class)->unexpected_msg_queue_mutex);

    HG_QUEUE_PUSH_TAIL(
        &NA_CCI_CLASS(na_class)->unexpected_msg_queue, rx, entry);

    hg_thread_mutex_unlock(&NA_CCI_CLASS(na_class)->unexpected_msg_queue_mutex);

out:
    return ret;
}

/*---------------------------------------------------------------------------*/
static struct na_cci_info_recv_unexpected *
na_cci_msg_unexpected_pop(na_class_t *na_class)
{
    struct na_cci_info_recv_unexpected *rx;

    hg_thread_mutex_lock(&NA_CCI_CLASS(na_class)->unexpected_msg_queue_mutex);

    rx = HG_QUEUE_FIRST(&NA_CCI_CLASS(na_class)->unexpected_msg_queue);
    HG_QUEUE_POP_HEAD(&NA_CCI_CLASS(na_class)->unexpected_msg_queue, entry);

    hg_thread_mutex_unlock(&NA_CCI_CLASS(na_class)->unexpected_msg_queue_mutex);

    return rx;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_msg_unexpected_op_push(
    na_class_t *na_class, na_cci_op_id_t *na_cci_op_id)
{
    na_return_t ret = NA_SUCCESS;

    if (!na_cci_op_id) {
        NA_LOG_ERROR("NULL operation ID");
        ret = NA_INVALID_PARAM;
        goto out;
    }
    hg_thread_mutex_lock(&NA_CCI_CLASS(na_class)->unexpected_op_queue_mutex);

    HG_QUEUE_PUSH_TAIL(
        &NA_CCI_CLASS(na_class)->unexpected_op_queue, na_cci_op_id, entry);

    hg_thread_mutex_unlock(&NA_CCI_CLASS(na_class)->unexpected_op_queue_mutex);

out:
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_cci_op_id_t *
na_cci_msg_unexpected_op_pop(na_class_t *na_class)
{
    na_cci_op_id_t *na_cci_op_id;

    hg_thread_mutex_lock(&NA_CCI_CLASS(na_class)->unexpected_op_queue_mutex);

    na_cci_op_id = HG_QUEUE_FIRST(&NA_CCI_CLASS(na_class)->unexpected_op_queue);
    HG_QUEUE_POP_HEAD(&NA_CCI_CLASS(na_class)->unexpected_op_queue, entry);

    hg_thread_mutex_unlock(&NA_CCI_CLASS(na_class)->unexpected_op_queue_mutex);

    return na_cci_op_id;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_msg_send_expected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, const void *buf, na_size_t buf_size,
    void NA_UNUSED *plugin_data, na_addr_t dest_addr,
    na_uint8_t NA_UNUSED dest_id, na_tag_t tag, na_op_id_t *op_id)
{
    na_cci_addr_t *na_cci_addr = (na_cci_addr_t *) dest_addr;
    na_cci_op_id_t *na_cci_op_id = (na_cci_op_id_t *) op_id;
    na_return_t ret = NA_SUCCESS;
    int rc;
    cci_msg_t msg;
    struct iovec iov[2];

    addr_addref(na_cci_addr); /* for na_cci_complete() */

    /* Check op_id */
    NA_CHECK_ERROR(
        na_cci_op_id == NULL, out, ret, NA_INVALID_ARG, "Invalid operation ID");
    NA_CHECK_ERROR(!hg_atomic_get32(&na_cci_op_id->completed), out, ret,
        NA_BUSY, "Attempting to use OP ID that was not completed");

    na_cci_op_id->context = context;
    na_cci_op_id->type = NA_CB_SEND_EXPECTED;
    na_cci_op_id->callback = callback;
    na_cci_op_id->arg = arg;
    hg_atomic_set32(&na_cci_op_id->completed, 0);
    hg_atomic_set32(&na_cci_op_id->canceled, 0);
    na_cci_op_id->info.send_expected.op_id = 0;

    msg.send.expect = 1;
    msg.send.bye = 0;
    msg.send.tag = tag;

    iov[0].iov_base = &msg;
    iov[0].iov_len = sizeof(msg.size);
    iov[1].iov_base = (void *) buf;
    iov[1].iov_len = buf_size;

    /* Post the CCI send request */
    rc = cci_sendv(na_cci_addr->cci_addr, iov, 2, na_cci_op_id, 0);
    if (rc) {
        cci_endpoint_t *endpoint = NA_CCI_CLASS(na_class)->endpoint;
        NA_LOG_ERROR("cci_sendv() failed with %s", cci_strerror(endpoint, rc));
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

out:
    if (ret != NA_SUCCESS) {
        addr_decref(na_cci_addr);
        hg_atomic_set32(&na_cci_op_id->completed, 1);
    }
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_msg_recv_expected(na_class_t NA_UNUSED *na_class, na_context_t *context,
    na_cb_t callback, void *arg, void *buf, na_size_t buf_size,
    void NA_UNUSED *plugin_data, na_addr_t source_addr,
    na_uint8_t NA_UNUSED source_id, na_tag_t tag, na_op_id_t *op_id)
{
    cci_size_t cci_buf_size = (cci_size_t) buf_size;
    na_cci_addr_t *na_cci_addr = (na_cci_addr_t *) source_addr;
    cci_msg_tag_t cci_tag = (cci_msg_tag_t) tag;
    struct na_cci_info_recv_expected *rx = NULL;
    na_cci_op_id_t *na_cci_op_id = (na_cci_op_id_t *) op_id;
    na_return_t ret = NA_SUCCESS;

    addr_addref(na_cci_addr); /* for na_cci_complete() */

    if (!na_cci_addr->cci_addr) {
        NA_LOG_ERROR("not connected to peer %s", na_cci_addr->uri);
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

    /* Check op_id */
    NA_CHECK_ERROR(
        na_cci_op_id == NULL, out, ret, NA_INVALID_ARG, "Invalid operation ID");
    NA_CHECK_ERROR(!hg_atomic_get32(&na_cci_op_id->completed), out, ret,
        NA_BUSY, "Attempting to use OP ID that was not completed");

    na_cci_op_id->context = context;
    na_cci_op_id->type = NA_CB_RECV_EXPECTED;
    na_cci_op_id->callback = callback;
    na_cci_op_id->arg = arg;
    hg_atomic_set32(&na_cci_op_id->completed, 0);
    hg_atomic_set32(&na_cci_op_id->canceled, 0);
    na_cci_op_id->info.recv_expected.na_cci_addr = na_cci_addr;
    na_cci_op_id->info.recv_expected.op_id = 0;
    na_cci_op_id->info.recv_expected.buf = buf;
    na_cci_op_id->info.recv_expected.buf_size = cci_buf_size;
    na_cci_op_id->info.recv_expected.actual_size = 0;
    na_cci_op_id->info.recv_expected.tag = cci_tag;

    /* See if it has already arrived */
    if (!HG_QUEUE_IS_EMPTY(&na_cci_addr->early)) {
        HG_QUEUE_FOREACH (rx, &na_cci_addr->early, entry) {
            if (rx->tag == cci_tag) {
                /* Found, copy to final buffer, and complete it */
                na_size_t len =
                    buf_size > rx->buf_size ? buf_size : rx->buf_size;
                memcpy(buf, rx->buf, len);
                na_cci_op_id->info.recv_expected.actual_size = len;
                HG_QUEUE_REMOVE(
                    &na_cci_addr->early, rx, na_cci_info_recv_expected, entry);
                free(rx->buf);
                free(rx);
                ret = na_cci_complete(na_cci_addr, na_cci_op_id, NA_SUCCESS);
                if (ret != NA_SUCCESS) {
                    NA_LOG_ERROR("Could not complete operation");
                }
                goto out;
            }
        }
    }

    /* Queue the recv request */
    HG_QUEUE_PUSH_TAIL(&na_cci_addr->rxs, na_cci_op_id, entry);

out:
    if (ret != NA_SUCCESS) {
        addr_decref(na_cci_addr);
        hg_atomic_set32(&na_cci_op_id->completed, 1);
    }
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_mem_handle_create(na_class_t NA_UNUSED *na_class, void *buf,
    na_size_t buf_size, unsigned long flags, na_mem_handle_t *mem_handle)
{
    na_ptr_t cci_buf_base = (na_ptr_t) buf;
    na_cci_mem_handle_t *na_cci_mem_handle = NULL;
    cci_size_t cci_buf_size = (cci_size_t) buf_size;
    na_return_t ret = NA_SUCCESS;

    /* Allocate memory handle (use calloc to avoid uninitialized transfer) */
    na_cci_mem_handle =
        (na_cci_mem_handle_t *) calloc(1, sizeof(na_cci_mem_handle_t));
    if (!na_cci_mem_handle) {
        NA_LOG_ERROR("Could not allocate NA CCI memory handle");
        ret = NA_NOMEM_ERROR;
        goto out;
    }

    na_cci_mem_handle->base = cci_buf_base;
    na_cci_mem_handle->size = cci_buf_size;
    na_cci_mem_handle->attr = (na_uint8_t) flags;

    *mem_handle = (na_mem_handle_t) na_cci_mem_handle;

out:
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_mem_handle_free(
    na_class_t NA_UNUSED *na_class, na_mem_handle_t mem_handle)
{
    na_cci_mem_handle_t *cci_mem_handle = (na_cci_mem_handle_t *) mem_handle;

    free(cci_mem_handle);

    return NA_SUCCESS;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_mem_register(na_class_t *na_class, na_mem_handle_t mem_handle)
{
    na_cci_mem_handle_t *na_cci_mem_handle = (na_cci_mem_handle_t *) mem_handle;
    cci_endpoint_t *e = NA_CCI_CLASS(na_class)->endpoint;
    cci_rma_handle_t *h = NULL;
    int rc = 0, flags = 0;
    na_return_t ret = NA_SUCCESS;

    switch (na_cci_mem_handle->attr) {
        case NA_MEM_READ_ONLY:
            flags = CCI_FLAG_READ;
            break;
        case NA_MEM_WRITE_ONLY:
            flags = CCI_FLAG_WRITE;
            break;
        case NA_MEM_READWRITE:
            flags = CCI_FLAG_READ | CCI_FLAG_WRITE;
            break;
        default:
            NA_LOG_ERROR("Invalid memory access flag");
            ret = NA_INVALID_PARAM;
            goto out;
    }

    rc = cci_rma_register(e, (void *) na_cci_mem_handle->base,
        na_cci_mem_handle->size, flags, &h);
    if (rc) {
        NA_LOG_ERROR("cci_rma_register() failed with %s", cci_strerror(e, rc));
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

    memcpy((void *) &na_cci_mem_handle->h, h, sizeof(*h));

out:
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_mem_deregister(na_class_t *na_class, na_mem_handle_t mem_handle)
{
    na_cci_mem_handle_t *na_cci_mem_handle = (na_cci_mem_handle_t *) mem_handle;
    cci_endpoint_t *e = NA_CCI_CLASS(na_class)->endpoint;
    int rc = 0;
    na_return_t ret = NA_SUCCESS;

    /* Check if remote handle */
    if (na_cci_mem_handle->base == 0 && na_cci_mem_handle->size == 0 &&
        na_cci_mem_handle->attr == 0)
        goto out;

    rc = cci_rma_deregister(e, &na_cci_mem_handle->h);
    if (rc) {
        NA_LOG_ERROR(
            "cci_rma_deregister() failed with %s", cci_strerror(e, rc));
        ret = NA_PROTOCOL_ERROR;
    }

out:
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_size_t
na_cci_mem_handle_get_serialize_size(
    na_class_t NA_UNUSED *na_class, na_mem_handle_t mem_handle)
{
    na_cci_mem_handle_t *na_cci_mem_handle = (na_cci_mem_handle_t *) mem_handle;

    /* We will only send the CCI RMA handle */
    return sizeof(na_cci_mem_handle->h);
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_mem_handle_serialize(na_class_t NA_UNUSED *na_class, void *buf,
    na_size_t buf_size, na_mem_handle_t mem_handle)
{
    na_cci_mem_handle_t *na_cci_mem_handle = (na_cci_mem_handle_t *) mem_handle;
    na_return_t ret = NA_SUCCESS;
    na_size_t len = sizeof(na_cci_mem_handle->h);

    if (buf_size < len) {
        NA_LOG_ERROR("Buffer size too small for serializing parameter");
        ret = NA_SIZE_ERROR;
        goto out;
    }

    /* The CCI RMA handle is already serialized. Just copy the cci_rma_handle_t.
     */

    /* Copy struct */
    memcpy(buf, &na_cci_mem_handle->h, len);

out:
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_mem_handle_deserialize(na_class_t NA_UNUSED *na_class,
    na_mem_handle_t *mem_handle, const void *buf, na_size_t buf_size)
{
    na_cci_mem_handle_t *na_cci_mem_handle = NULL;
    na_return_t ret = NA_SUCCESS;
    na_size_t len = sizeof(na_cci_mem_handle->h);

    if (buf_size < len) {
        NA_LOG_ERROR("Buffer size too small for deserializing parameter");
        ret = NA_SIZE_ERROR;
        goto out;
    }
    na_cci_mem_handle = calloc(1, sizeof(*na_cci_mem_handle));
    if (!na_cci_mem_handle) {
        NA_LOG_ERROR("Could not allocate NA CCI memory handle");
        ret = NA_NOMEM_ERROR;
        goto out;
    }

    /* The CCI RMA handle is ready to use. Just copy it into a
     * na_cci_mem_handle_t and never access the other fields
     * when it is a remote handle. */

    /* Copy struct */
    memcpy((void *) &na_cci_mem_handle->h, buf, len);

    *mem_handle = (na_mem_handle_t) na_cci_mem_handle;

out:
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_put(na_class_t *na_class, na_context_t *context, na_cb_t callback,
    void *arg, na_mem_handle_t local_mem_handle, na_offset_t local_offset,
    na_mem_handle_t remote_mem_handle, na_offset_t remote_offset,
    na_size_t length, na_addr_t remote_addr, na_uint8_t NA_UNUSED remote_id,
    na_op_id_t *op_id)
{
    na_cci_mem_handle_t *cci_local_mem_handle =
        (na_cci_mem_handle_t *) local_mem_handle;
    na_cci_mem_handle_t *cci_remote_mem_handle =
        (na_cci_mem_handle_t *) remote_mem_handle;
    na_cci_addr_t *na_cci_addr = (na_cci_addr_t *) remote_addr;
    na_cci_op_id_t *na_cci_op_id = (na_cci_op_id_t *) op_id;
    na_return_t ret = NA_SUCCESS;
    int rc;
    cci_endpoint_t *e = NA_CCI_CLASS(na_class)->endpoint;
    cci_connection_t *c = na_cci_addr->cci_addr;
    cci_rma_handle_t *local = &cci_local_mem_handle->h;
    cci_rma_handle_t *remote = &cci_remote_mem_handle->h;

    addr_addref(na_cci_addr); /* for na_cci_complete() */

    if (!na_cci_addr->cci_addr) {
        NA_LOG_ERROR("not connected to peer %s", na_cci_addr->uri);
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

    /* Check op_id */
    NA_CHECK_ERROR(
        na_cci_op_id == NULL, out, ret, NA_INVALID_ARG, "Invalid operation ID");
    NA_CHECK_ERROR(!hg_atomic_get32(&na_cci_op_id->completed), out, ret,
        NA_BUSY, "Attempting to use OP ID that was not completed");

    na_cci_op_id->context = context;
    na_cci_op_id->type = NA_CB_PUT;
    na_cci_op_id->callback = callback;
    na_cci_op_id->arg = arg;
    hg_atomic_set32(&na_cci_op_id->completed, 0);
    hg_atomic_set32(&na_cci_op_id->canceled, 0);
    na_cci_op_id->info.put.request_op_id = 0;
    na_cci_op_id->info.put.transfer_op_id = 0;
    na_cci_op_id->info.put.transfer_completed = NA_FALSE;
    na_cci_op_id->info.put.transfer_actual_size = 0;
    na_cci_op_id->info.put.completion_op_id = 0;
    na_cci_op_id->info.put.completion_actual_size = 0;
    na_cci_op_id->info.put.internal_progress = NA_FALSE;
    na_cci_op_id->info.put.remote_addr = na_cci_addr->cci_addr;

    /* Post the CCI RMA */
    rc = cci_rma(c, NULL, 0, local, local_offset, remote, remote_offset, length,
        na_cci_op_id, CCI_FLAG_WRITE);
    if (rc) {
        NA_LOG_ERROR("cci_rma() failed with %s", cci_strerror(e, rc));
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

out:
    if (ret != NA_SUCCESS) {
        addr_decref(na_cci_addr);
        hg_atomic_set32(&na_cci_op_id->completed, 1);
    }
    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_get(na_class_t *na_class, na_context_t *context, na_cb_t callback,
    void *arg, na_mem_handle_t local_mem_handle, na_offset_t local_offset,
    na_mem_handle_t remote_mem_handle, na_offset_t remote_offset,
    na_size_t length, na_addr_t remote_addr, na_uint8_t NA_UNUSED remote_id,
    na_op_id_t *op_id)
{
    na_cci_mem_handle_t *cci_local_mem_handle =
        (na_cci_mem_handle_t *) local_mem_handle;
    na_cci_mem_handle_t *cci_remote_mem_handle =
        (na_cci_mem_handle_t *) remote_mem_handle;
    na_cci_addr_t *na_cci_addr = (na_cci_addr_t *) remote_addr;
    na_cci_op_id_t *na_cci_op_id = (na_cci_op_id_t *) op_id;
    na_return_t ret = NA_SUCCESS;
    int rc;
    cci_endpoint_t *e = NA_CCI_CLASS(na_class)->endpoint;
    cci_connection_t *c = na_cci_addr->cci_addr;
    cci_rma_handle_t *local = &cci_local_mem_handle->h;
    cci_rma_handle_t *remote = &cci_remote_mem_handle->h;

    addr_addref(na_cci_addr); /* for na_cci_complete() */

    if (!na_cci_addr->cci_addr) {
        NA_LOG_ERROR("not connected to peer %s", na_cci_addr->uri);
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

    /* Check op_id */
    NA_CHECK_ERROR(
        na_cci_op_id == NULL, out, ret, NA_INVALID_ARG, "Invalid operation ID");
    NA_CHECK_ERROR(!hg_atomic_get32(&na_cci_op_id->completed), out, ret,
        NA_BUSY, "Attempting to use OP ID that was not completed");

    na_cci_op_id->context = context;
    na_cci_op_id->type = NA_CB_GET;
    na_cci_op_id->callback = callback;
    na_cci_op_id->arg = arg;
    hg_atomic_set32(&na_cci_op_id->completed, 0);
    hg_atomic_set32(&na_cci_op_id->canceled, 0);
    na_cci_op_id->info.get.request_op_id = 0;
    na_cci_op_id->info.get.transfer_op_id = 0;
    na_cci_op_id->info.get.transfer_actual_size = 0;
    na_cci_op_id->info.get.internal_progress = NA_FALSE;
    na_cci_op_id->info.get.remote_addr = na_cci_addr->cci_addr;

    /* Post the CCI RMA */
    rc = cci_rma(c, NULL, 0, local, local_offset, remote, remote_offset, length,
        na_cci_op_id, CCI_FLAG_READ);
    if (rc) {
        NA_LOG_ERROR("cci_rma() failed %s", cci_strerror(e, rc));
        ret = NA_PROTOCOL_ERROR;
        goto out;
    }

out:
    if (ret != NA_SUCCESS) {
        addr_decref(na_cci_addr);
        hg_atomic_set32(&na_cci_op_id->completed, 1);
    }
    return ret;
}

/*---------------------------------------------------------------------------*/
static int
na_cci_poll_get_fd(na_class_t *na_class, na_context_t NA_UNUSED *context)
{
    return NA_CCI_CLASS(na_class)->fd;
}

/*---------------------------------------------------------------------------*/
static void
handle_send(na_class_t NA_UNUSED *class, na_context_t NA_UNUSED *context,
    cci_endpoint_t NA_UNUSED *e, cci_event_t *event)
{
    na_cci_op_id_t *na_cci_op_id = event->send.context;
    na_return_t ret =
        event->send.status == CCI_SUCCESS ? NA_SUCCESS : NA_PROTOCOL_ERROR;
    na_cci_addr_t *na_cci_addr = event->send.connection->context;

    if (!na_cci_op_id) {
        NA_LOG_ERROR("NULL operation ID");
        goto out;
    } else if (hg_atomic_get32(&na_cci_op_id->canceled) && ret == NA_SUCCESS) {
        ret = NA_CANCELED;
    }

    ret = na_cci_complete(na_cci_addr, na_cci_op_id, ret);
    if (ret != NA_SUCCESS)
        NA_LOG_ERROR("Unable to complete send");

out:
    return;
}

/*---------------------------------------------------------------------------*/
static void
handle_recv_expected(na_class_t NA_UNUSED *na_class,
    na_context_t NA_UNUSED *context, cci_endpoint_t NA_UNUSED *e,
    cci_event_t *event)
{
    cci_connection_t *c = event->recv.connection;
    na_cci_addr_t *na_cci_addr = c->context;
    cci_msg_t *msg = (void *) event->recv.ptr;
    na_size_t msg_len = event->recv.len - sizeof(msg->size);
    na_cci_op_id_t *na_cci_op_id = NULL;
    struct na_cci_info_recv_expected *rx = NULL;
    int rc = 0;
    na_return_t ret;

    HG_QUEUE_FOREACH (na_cci_op_id, &na_cci_addr->rxs, entry) {
        if (na_cci_op_id->info.recv_expected.tag == msg->send.tag) {
            na_size_t len = msg_len;

            if (na_cci_op_id->info.recv_expected.buf_size < len)
                len = na_cci_op_id->info.recv_expected.buf_size;
            memcpy(na_cci_op_id->info.recv_expected.buf, msg->send.data, len);
            na_cci_op_id->info.recv_expected.actual_size = len;
            HG_QUEUE_REMOVE(
                &na_cci_addr->rxs, na_cci_op_id, na_cci_op_id, entry);
            ret = na_cci_complete(na_cci_addr, na_cci_op_id, NA_SUCCESS);
            if (ret != NA_SUCCESS) {
                NA_LOG_ERROR("Could not complete expected recv");
            }
            goto out;
        }
    }

    /* Early receive, cache it */
    rx = calloc(1, sizeof(*rx));
    if (!rx) {
        NA_LOG_ERROR("Unable to allocate expected recv - dropping recv");
        goto out;
    }

    rx->buf = calloc(1, msg_len);
    if (!rx->buf) {
        rc = CCI_ENOMEM;
        goto out;
    }

    memcpy(rx->buf, msg->send.data, msg_len);
    rx->buf_size = rx->actual_size = msg_len;
    rx->tag = msg->send.tag;

    HG_QUEUE_PUSH_TAIL(&na_cci_addr->early, rx, entry);

out:
    if (rc) {
        if (rx)
            free(rx->buf);
        free(rx);
    }
    return;
}

/*---------------------------------------------------------------------------*/
static void
handle_recv_unexpected(na_class_t *na_class, na_context_t NA_UNUSED *context,
    cci_endpoint_t NA_UNUSED *e, cci_event_t *event)
{
    cci_connection_t *c = event->recv.connection;
    na_cci_addr_t *na_cci_addr = c->context;
    cci_msg_t *msg = (void *) event->recv.ptr;
    na_size_t msg_len = event->recv.len - sizeof(msg->size);
    na_cci_op_id_t *na_cci_op_id = NULL;
    struct na_cci_info_recv_unexpected *rx = NULL;
    int rc = 0;
    na_return_t ret = NA_SUCCESS;

    if (!na_cci_addr->cci_addr || hg_atomic_get32(&na_cci_addr->refcnt) <= 0) {
        NA_LOG_ERROR("peer %s refcnt %d\n", na_cci_addr->uri,
            hg_atomic_get32(&na_cci_addr->refcnt));
        goto out;
    }

    addr_addref(na_cci_addr); /* ref for na_cci_complete() */

    na_cci_op_id = na_cci_msg_unexpected_op_pop(na_class);

    if (na_cci_op_id) {
        na_size_t len = na_cci_op_id->info.recv_unexpected.buf_size <
                                event->recv.len - msg_len
                            ? na_cci_op_id->info.recv_unexpected.buf_size
                            : msg_len;
        na_cci_op_id->info.recv_unexpected.na_cci_addr = na_cci_addr;
        na_cci_op_id->info.recv_unexpected.actual_size = len;
        na_cci_op_id->info.recv_unexpected.tag = msg->send.tag;
        memcpy(na_cci_op_id->info.recv_unexpected.buf, msg->send.data, len);

        addr_addref(na_cci_addr); /* for na_cci_addr_free() */

        ret = na_cci_complete(na_cci_addr, na_cci_op_id, NA_SUCCESS);
        if (ret != NA_SUCCESS) {
            NA_LOG_ERROR("failed to complete unexpected recv");
            goto out;
        }
    } else {
        rx = calloc(1, sizeof(*rx));
        if (!rx) {
            NA_LOG_ERROR("Could not allocate memory for unexpected recv - "
                         "dropping the message");
            rc = CCI_ENOMEM;
            goto out;
        }
        rx->buf = calloc(1, msg_len);
        if (!rx->buf) {
            NA_LOG_ERROR("Could not allocate memory for unexpected recv - "
                         "dropping the message");
            rc = CCI_ENOMEM;
            goto out;
        }
        memcpy(rx->buf, msg->send.data, msg_len);
        rx->buf_size = rx->actual_size = msg_len;
        rx->na_cci_addr = na_cci_addr;
        rx->tag = msg->send.tag;

        ret = na_cci_msg_unexpected_push(na_class, rx);
        if (ret != NA_SUCCESS) {
            NA_LOG_ERROR("Unable to push unexpected recv");
            rc = CCI_ERROR;
        }
    }

out:
    if (rc) {
        if (rx)
            free(rx->buf);
        free(rx);
    }

    return;
}

/*---------------------------------------------------------------------------*/
static void
handle_recv(na_class_t *na_class, na_context_t *context, cci_endpoint_t *e,
    cci_event_t *event)
{
    cci_msg_t *msg = (void *) event->recv.ptr;

    if (msg->send.expect) {
        handle_recv_expected(na_class, context, e, event);
    } else {
        handle_recv_unexpected(na_class, context, e, event);
    }

    return;
}

/*---------------------------------------------------------------------------*/
static void
handle_connect_request(na_class_t NA_UNUSED *class,
    na_context_t NA_UNUSED *context, cci_endpoint_t *e, cci_event_t *event)
{
    na_cci_addr_t *na_cci_addr = NULL;
    int rc = 0;

    na_cci_addr = calloc(1, sizeof(*na_cci_addr));
    if (!na_cci_addr) {
        NA_LOG_ERROR("Unable to allocate na_cci_addr for new peer %s",
            (const char *) (event->request.data_ptr));
        rc = CCI_ENOMEM;
        goto out;
    }

    HG_QUEUE_INIT(&na_cci_addr->rxs);
    HG_QUEUE_INIT(&na_cci_addr->early);

    na_cci_addr->uri = strdup(event->request.data_ptr);
    if (!na_cci_addr->uri) {
        NA_LOG_ERROR("Unable to allocate URI for new peer %s",
            (const char *) (event->request.data_ptr));
        rc = CCI_ENOMEM;
        goto out;
    }

    hg_atomic_set32(&na_cci_addr->refcnt, 1);

    na_cci_addr->unexpected = NA_TRUE;
    na_cci_addr->self = NA_FALSE;

    rc = cci_accept(event, (void *) na_cci_addr);
    if (rc)
        NA_LOG_ERROR("cci_acept() failed with %s", cci_strerror(e, rc));

out:
    if (rc) {
        if (na_cci_addr)
            free(na_cci_addr->uri);
        free(na_cci_addr);
    }
    return;
}

/*---------------------------------------------------------------------------*/
static void
handle_connect(na_class_t NA_UNUSED *class, na_context_t NA_UNUSED *context,
    cci_endpoint_t *e, cci_event_t *event)
{
    na_cci_addr_t *na_cci_addr = event->connect.context;

    if (event->connect.status != CCI_SUCCESS) {
        NA_LOG_ERROR("connect to %s failed with %s", na_cci_addr->uri,
            cci_strerror(e, event->connect.status));
    } else {
        na_cci_addr->cci_addr = event->connect.connection;
    }

    return;
}

/*---------------------------------------------------------------------------*/
static void
handle_accept(na_class_t NA_UNUSED *na_class, na_context_t NA_UNUSED *context,
    cci_endpoint_t NA_UNUSED *e, cci_event_t *event)
{
    na_cci_addr_t *na_cci_addr = event->accept.context;

    na_cci_addr->cci_addr = event->accept.connection;

    /* Add address to accepted connection list */
    hg_thread_mutex_lock(&NA_CCI_CLASS(na_class)->accept_conn_list_mutex);
    HG_LIST_INSERT_HEAD(
        &NA_CCI_CLASS(na_class)->accept_conn_list, na_cci_addr, entry);
    hg_thread_mutex_unlock(&NA_CCI_CLASS(na_class)->accept_conn_list_mutex);

    return;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_progress(
    na_class_t *na_class, na_context_t *context, unsigned int timeout)
{
    double remaining =
        timeout / 1000.0; /* Convert timeout in ms into seconds */
    na_return_t ret = NA_TIMEOUT;
    cci_endpoint_t *e = NA_CCI_CLASS(na_class)->endpoint;

    do {
        int rc;
        hg_time_t t1, t2;
        cci_event_t *event = NULL;

        if (timeout)
            hg_time_get_current_ms(&t1);

        rc = cci_get_event(e, &event);
        if (rc) {
            if (rc != CCI_EAGAIN)
                NA_LOG_ERROR(
                    "cci_return_event() failed %s", cci_strerror(e, rc));

            if (timeout) {
                hg_time_get_current_ms(&t2);
                remaining -= hg_time_diff(t2, t1);
            }
            if (remaining <= 0)
                break; /* Return NA_TIMEOUT */
            continue;
        }

        /* We got an event, handle it */
        switch (event->type) {
            case CCI_EVENT_SEND:
                handle_send(na_class, context, e, event);
                break;
            case CCI_EVENT_RECV:
                handle_recv(na_class, context, e, event);
                break;
            case CCI_EVENT_CONNECT_REQUEST:
                handle_connect_request(na_class, context, e, event);
                break;
            case CCI_EVENT_CONNECT:
                handle_connect(na_class, context, e, event);
                break;
            case CCI_EVENT_ACCEPT:
                handle_accept(na_class, context, e, event);
                break;
            default:
                NA_LOG_ERROR(
                    "unhandled %s event", cci_event_type_str(event->type));
        }

        /* We progressed, return success */
        ret = NA_SUCCESS;

        rc = cci_return_event(event);
        if (rc)
            NA_LOG_ERROR("cci_return_event() failed %s", cci_strerror(e, rc));

    } while (remaining > 0 && ret != NA_SUCCESS);

    return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_complete(
    na_cci_addr_t *na_cci_addr, na_cci_op_id_t *na_cci_op_id, na_return_t ret)
{
    struct na_cb_info *callback_info = NULL;

    /* Mark op id as completed */
    hg_atomic_incr32(&na_cci_op_id->completed);

    /* Init callback info */
    callback_info = &na_cci_op_id->completion_data.callback_info;
    callback_info->arg = na_cci_op_id->arg;
    callback_info->ret = ret;
    callback_info->type = na_cci_op_id->type;

    switch (na_cci_op_id->type) {
        case NA_CB_RECV_UNEXPECTED: {
            /* Fill callback info */
            callback_info->info.recv_unexpected.actual_buf_size =
                (na_size_t) na_cci_op_id->info.recv_unexpected.actual_size;
            callback_info->info.recv_unexpected.source =
                (na_addr_t) na_cci_op_id->info.recv_unexpected.na_cci_addr;
            callback_info->info.recv_unexpected.tag =
                (na_tag_t) na_cci_op_id->info.recv_unexpected.tag;
        } break;
        case NA_CB_RECV_EXPECTED:
            /* Check buf_size and actual_size */
            if (na_cci_op_id->info.recv_expected.actual_size >
                na_cci_op_id->info.recv_expected.buf_size) {
                NA_LOG_ERROR("Expected recv too large for buffer");
                ret = NA_SIZE_ERROR;
                goto out;
            }
            break;
        case NA_CB_SEND_UNEXPECTED:
        case NA_CB_SEND_EXPECTED:
            break;
        case NA_CB_PUT:
            break;
        case NA_CB_GET:
            break;
        default:
            NA_LOG_ERROR("Operation not supported");
            ret = NA_INVALID_PARAM;
            break;
    }

    na_cci_op_id->completion_data.callback = na_cci_op_id->callback;
    na_cci_op_id->completion_data.plugin_callback = na_cci_release;
    na_cci_op_id->completion_data.plugin_callback_args = na_cci_op_id;

    na_cb_completion_add(na_cci_op_id->context, &na_cci_op_id->completion_data);

out:
    if (na_cci_addr)
        addr_decref(na_cci_addr);
    return ret;
}

/*---------------------------------------------------------------------------*/
static void
na_cci_release(void *arg)
{
    na_cci_op_id_t *na_cci_op_id = (na_cci_op_id_t *) arg;

    if (na_cci_op_id && !hg_atomic_get32(&na_cci_op_id->completed)) {
        NA_LOG_WARNING("Releasing resources from an uncompleted operation");
    }
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_cci_cancel(
    na_class_t *na_class, na_context_t NA_UNUSED *context, na_op_id_t *op_id)
{
    na_cci_op_id_t *na_cci_op_id = (na_cci_op_id_t *) op_id;
    na_cci_addr_t *na_cci_addr = NULL;
    na_return_t ret = NA_SUCCESS;

    if (hg_atomic_get32(&na_cci_op_id->completed))
        goto out;

    hg_atomic_incr32(&na_cci_op_id->canceled);

    switch (na_cci_op_id->type) {
        case NA_CB_RECV_UNEXPECTED: {
            na_cci_op_id_t *tmp = NULL, *first = NULL;

            tmp = first = na_cci_msg_unexpected_op_pop(na_class);

            do {
                if (!tmp) {
                    ret = NA_PROTOCOL_ERROR;
                    goto out;
                }

                if (tmp == na_cci_op_id)
                    break;

                na_cci_msg_unexpected_op_push(na_class, tmp);

                tmp = na_cci_msg_unexpected_op_pop(na_class);
                if (tmp == first) {
                    ret = NA_PROTOCOL_ERROR;
                    goto out;
                }
            } while (tmp != na_cci_op_id);
        } break;
        case NA_CB_RECV_EXPECTED: {
            int found = 0;
            na_cci_op_id_t *tmp = NULL;

            na_cci_addr = na_cci_op_id->info.recv_expected.na_cci_addr;

            HG_QUEUE_FOREACH (tmp, &na_cci_addr->rxs, entry) {
                if (tmp == na_cci_op_id) {
                    HG_QUEUE_REMOVE(
                        &na_cci_addr->rxs, tmp, na_cci_op_id, entry);
                    found = 1;
                }
            }
            if (!found) {
                ret = NA_PROTOCOL_ERROR;
                goto out;
            }
        } break;
        case NA_CB_SEND_UNEXPECTED:
        case NA_CB_SEND_EXPECTED:
        case NA_CB_PUT:
        case NA_CB_GET:
            goto out;
            break;
        default:
            break;
    }

    ret = na_cci_complete(na_cci_addr, na_cci_op_id, NA_CANCELED);

out:
    return ret;
}
