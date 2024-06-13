/*
 * (C) 2015 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef __MY_RPC
#define __MY_RPC

#include <margo.h>
#include <map>
/* visible API for example RPC operation */
struct fs_info {
    public: 
        unsigned int priority, post_priority;
        std::vector<std::string> daemon_addrs;
    
    bool operator<(const fs_info& other) const {
        if (priority != other.priority) {
            return priority > other.priority;
        }
        return post_priority > other.post_priority;
    }
};

static std::map< std::string, std::pair<std::string, std::string> > job_flows; 

DECLARE_MARGO_RPC_HANDLER(rpc_srv_registry_request)

DECLARE_MARGO_RPC_HANDLER(rpc_srv_registry_register)

#endif /* __MY_RPC */
