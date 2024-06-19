/*
 * (C) 2015 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <mercury.h>
#include <margo.h>
#include <margo-logging.h>
#include <CLI/CLI.hpp>
#include <iostream>
#include <fstream>
#include <fmt/format.h>

#include <common/env_util.hpp>
#include <common/rpc/rpc_types.hpp>
#include <common/rpc/rpc_util.hpp>
#include <common/statistics/stats.hpp>
#include <common/common_defs.hpp>

#include <registry/my-rpc.hpp>
#include <registry/env.hpp>

using namespace std;
/* example server program.  Starts HG engine, registers the example RPC type,
 * and then executes indefinitely.
 */
struct cli_options {
    string register_path;
    string rpc_protocol;
};


int main(int argc, char** argv)
{
    string registry_file  = gkfs::env::get_var(gkfs::env::REGISTRY_FILE,
                                        gkfs::config::registryfile_path);
    
    auto rpc_protocol = string(gkfs::rpc::protocol::ofi_sockets);

    std::vector<std::string> params(argv, argv + argc);
    for (size_t i = 1; i < params.size(); ++i) {
        if (params[i] == "-P" && i + 1 < params.size()) {
            rpc_protocol = params[i + 1];
            break;
        }
    }
    if(rpc_protocol != gkfs::rpc::protocol::ofi_verbs &&
        rpc_protocol != gkfs::rpc::protocol::ofi_sockets &&
        rpc_protocol != gkfs::rpc::protocol::ofi_psm2 &&
        rpc_protocol != gkfs::rpc::protocol::na_ucx){
            throw runtime_error(fmt::format(
                    "Given RPC protocol '{}' not supported. ",
                    rpc_protocol));
        }

    hg_return_t            hret;
    margo_instance_id      mid;
    hg_addr_t              addr_self;
    char                   addr_self_string[128];
    hg_size_t              addr_self_string_sz = 128;
    char*                  starter_json        = "{\"output_dir\":\"/tmp\"}";


    struct margo_init_info args = {nullptr};
    args.json_config = starter_json;
    mid               = margo_init_ext(rpc_protocol.c_str(), MARGO_SERVER_MODE, &args);
    if (mid == MARGO_INSTANCE_NULL) {
        fprintf(stderr, "Error: margo_init_ext()\n");
        return (-1);
    }

    margo_diag_start(mid);
    margo_profile_start(mid);

    /* figure out what address this server is listening on */
    hret = margo_addr_self(mid, &addr_self);
    if (hret != HG_SUCCESS) {
        fprintf(stderr, "Error: margo_addr_self()\n");
        margo_finalize(mid);
        return (-1);
    }
    hret = margo_addr_to_string(mid, addr_self_string, &addr_self_string_sz,
                                addr_self);
    if (hret != HG_SUCCESS) {
        fprintf(stderr, "Error: margo_addr_to_string()\n");
        margo_addr_free(mid, addr_self);
        margo_finalize(mid);
        return (-1);
    }
    margo_addr_free(mid, addr_self);

    ofstream lf(registry_file, ios::out);
    if(!lf) {
        throw runtime_error(fmt::format("Failed to open hosts file '{}': {}",
                                        registry_file, strerror(errno)));
    }
    lf << addr_self_string << endl;
    if(!lf) {
        throw runtime_error(
                fmt::format("Failed to write on hosts file '{}': {}",
                            registry_file, strerror(errno)));
    }
    lf.close();

    fprintf(stderr, "# accepting RPCs on address \"%s\"\n", addr_self_string);
    //fprintf(stderr, "# connect to this server with \"./margo-example-client %s\"\n", addr_self_string);

    /* register RPC */
    MARGO_REGISTER(mid, gkfs::rpc::tag::registry_request, rpc_registry_request_in_t, rpc_registry_request_out_t, 
                    rpc_srv_registry_request);
    MARGO_REGISTER(mid, gkfs::rpc::tag::registry_register, rpc_registry_register_in_t, rpc_err_out_t, 
                    rpc_srv_registry_register);
    

#if 0
    /* this could be used to display json configuration at run time */
    char*             cfg_str;
    cfg_str = margo_get_config(mid);
    printf("%s\n", cfg_str);
    free(cfg_str);
#endif
    margo_wait_for_finalize(mid);

    return (0);
}
