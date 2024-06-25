/*
 * (C) 2015 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <iostream>
#include <vector>
#include <sstream>
#include <fstream>
#include <set>
#include <queue>
#include <registry/my-rpc.hpp>

#include <common/rpc/rpc_types.hpp>

static int merge_files(){
    return 0;

}

/**
 * @brief Responds to merge request from client 
 * with merge GekkoFS information written into hostfile and hostconfigfile
 * @internal
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_registry_request(hg_handle_t handle)
{   
    //std::cout<< "succeed in getting flows hfile hcfile\n" <<std::endl;
    rpc_registry_request_in_t in;
    rpc_registry_request_out_t out;out.err = 0;
    std::vector<std::string> flow_arr = {};

    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS)
    assert(ret == HG_SUCCESS);
    auto flows = in.merge_flows;
    auto hfile = in.merge_hfile;
    auto hcfile = in.merge_hcfile;

    try {
            std::stringstream ss(flows);
            std::string flow;
            //Separate the flows with; , flow_arr stores all the work flows to be soon merged
            while (std::getline(ss, flow, ';')) {
                flow_arr.push_back(flow);
            }
            std::priority_queue<fs_info> all_fs_info; // save fs information (priority and daemons vector) sorted by the priority of fs
            std::set<std::string> all_daemons;//check for duplicate daemons and suitable for multi-layer fusion GekkoFS
            //search the hfiles and hcfiles of merge_flows and merge them to merge_hfile and merge_hcfile
            for(int i = 0 ; i < flow_arr.size(); i ++){
                if(!job_flows.count(flow_arr[i])) {
                    std::cout<< "we can't find flow names " << flow_arr[i] <<std::endl;
                    continue;
                }
                // open hfile and hcfile of flow i
                std::ifstream hcf(job_flows[flow_arr[i]].first);
                std::ifstream hf(job_flows[flow_arr[i]].second);

                if (hcf.is_open() && hf.is_open()) {
                    std::string line;
                    while (std::getline(hcf, line)) { //every line contains two number: fs daemons count and fs priority 
                        struct fs_info fsinfo;
                        fsinfo.priority = i;

                        std::stringstream ss(line);
                        unsigned int fsdaemons,fspriority;
                        ss>> fsdaemons >> fspriority;
                        fsinfo.post_priority = fspriority;

                        for (int k = 0; k < fsdaemons; ++k) { //save all daemon addrs of this fs 
                            if (std::getline(hf, line)) { //every line contains a daemon addr
                                if(all_daemons.count(line))
                                    continue;
                                all_daemons.insert(line);
                                fsinfo.daemon_addrs.push_back(line);
                            }
                                
                        }
                        if(fsinfo.daemon_addrs.size() > 0)
                            all_fs_info.push(fsinfo);
                        
                    }
                }
                hcf.close();
                hf.close();
            }
        //写入文件
        std::ofstream hcf(hcfile);
        std::ofstream hf(hfile);
        unsigned int prior = 1;
        while(!all_fs_info.empty()){
            fs_info fsinfo = all_fs_info.top();
            all_fs_info.pop();
            hcf << fsinfo.daemon_addrs.size() << " " << prior <<std::endl;
            prior ++;
            for(auto addr : fsinfo.daemon_addrs){
                hf << addr << std::endl;
            }
        }
        hcf.close();
        hf.close();

    }   catch(const std::exception& e) {
            std::cout<< "Failed to respond my rpc ult\n" << e.what();
            out.err = -1;
    }

    //std::cout<< "request out err " << out.err <<std::endl;
    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        std::cout<< "Failed to respond my rpc ult\n";
    }

    margo_free_input(handle, &in);
    margo_destroy(handle);
    return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(rpc_srv_registry_request)

/**
 * @brief Responds to workflow register from client, save system info where workflow is located
 * @internal
 * @param handle Mercury RPC handle
 * @return Mercury error code to Mercury
 */
hg_return_t
rpc_srv_registry_register(hg_handle_t handle)
{
    //std::cout<< "succeed in getting flows hfile hcfile " <<std::endl;
    rpc_registry_register_in_t in;
    rpc_err_out_t out;

    auto ret = margo_get_input(handle, &in);
    if(ret != HG_SUCCESS)
    assert(ret == HG_SUCCESS);
    auto flow = in.work_flow;
    auto hfile = in.hfile;
    auto hcfile = in.hcfile;
    try {
        //Store the hostconfigfile and hostfile of the system where the workflow is located
        std::cout<< flow<<" " << hcfile <<" "<< hfile <<std::endl;
        job_flows[flow] = {hcfile,hfile} ;
    } catch(const std::exception& e) {
        out.err = -1;
    }
    //std::cout<< "register out err " << out.err <<std::endl;
    auto hret = margo_respond(handle, &out);
    if(hret != HG_SUCCESS) {
        std::cout<< "Failed to respond my rpc ult\n";
    }
    margo_free_input(handle, &in);
    margo_destroy(handle);
    return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(rpc_srv_registry_register)
