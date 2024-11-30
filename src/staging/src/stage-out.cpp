#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mpi.h>
#include <fstream>
#include <string>
#include <iostream>
#include <functional>
#include <libgen.h>
#include <string.h>

#include <map>
#include<vector>
#include <thread>
#include <sstream>
#include <sys/stat.h>


#include <config.hpp>
#include <common/arithmetic/arithmetic.hpp>



#define CHUNK_SIZE gkfs::config::rpc::chunksize // using chunksize of gekkofs
#define THREAD_NUM 16
double rs_time, re_time;
void read_from_file( const std::vector<uint64_t>& chunk_ids, const long long start_index, const long long end_index, const std::string& base_dir, FILE* output) {
    for (long long i = start_index; i < end_index + 1; i++){
        std::string filename = base_dir + std::to_string(chunk_ids[i]);
        std::ifstream file(filename, std::ios::binary );
        if (file.is_open()) {
            std::vector<char> writeBuffer(CHUNK_SIZE);
            file.read(writeBuffer.data(), CHUNK_SIZE);
//            std::cout << "recv size:" << file.gcount() << std::endl;
            fseek(output, chunk_ids[i]*CHUNK_SIZE, SEEK_SET);
            fwrite(writeBuffer.data(), sizeof(char),CHUNK_SIZE,output);
            //fflush(output);
            file.close();
            //std::cout << "write to " << filename << std::endl;
        } else {
            std::cerr << "error with open " << filename << std::endl;
        }
    }
}

std::string getLastPart(const std::string& line) {
    std::stringstream stream(line);
    std::string segment;
    std::vector<std::string> parts;
    //std::cout << line << std::endl;
    while (std::getline(stream, segment, ':')) {
        parts.push_back(segment);
    }

    if (!parts.empty()) {
//		std::cout << "port: " <<  parts.back() << std::endl;
        return parts.back();
    }

    return "";  // 如果没有分割结果，则返回空字符串
}

std::string getLineFromFile(const std::string& filename, int lineNum) {
    std::ifstream file(filename);
    std::string line;
    int currentLine = 0;

    while (std::getline(file, line)) {
        if (currentLine == lineNum) {
            //std::cout << "line: " << std::endl;
            file.close();
            return line;
        }
        //std::cout << "currentLine: "<<  currentLine << std::endl;
        currentLine++;
    }

    file.close();
    std::cout << "not found line, error " << filename << " " << lineNum << std::endl;
    return "";  // 如果指定行不存在，则返回空字符串
}

std::string get_port_of_daemon(const std::string& filename, int lineNum) {
    std::string line = getLineFromFile(filename, lineNum);
    if (!line.empty()) {
        std::string lastPart = getLastPart(line);
        return lastPart;
    } else {
        std::cout << "port not found in " << filename << std::endl;
    }
}




std::string get_pid_by_port(std::string port) {
    std::string command = "lsof -i :" + port + " | tail -n 1 | awk '{print $2}' | head -n 1 ";
    //std::cout << "cmd: " << command << std::endl;
    std::string result;

    FILE* pipe = popen(command.c_str(), "r"); // hook! TODO
    if (pipe) {
        char buffer[128];
        while (!feof(pipe)) {
            if (fgets(buffer, 128, pipe) != nullptr)
                result += buffer;
        }
        pclose(pipe);
    }
    std::cout << "pid: " << result << std::endl;
    return result;
}





std::string get_daemon_pid_by_rank(const std::string& filename, int rank) {
    std::string port = get_port_of_daemon(filename, rank);
    //std::string pid = get_pid_by_port(port);
    return port; // .pid file return pid TODO
}




// args: input_file output_file /path-to/gkfs_hosts.txt /gkfs-data-dir/
// [1] input_file
// [2] output_file
// [3] /path-to/gkfs_hosts.txt.pid
// [4] /gkfs-data-dir/   eg: /tmp/gekkofs/data

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cout << "args: source_file target_file /path-to/gkfs_hosts.txt.pid /gkfs-data-dir/ " << std::endl;
        exit(1);
    }

    int host_size, thread_num;
    char *tmp_env;
    if ((tmp_env = getenv("HOST_SIZE")) != NULL) {
        host_size = atoi(tmp_env);
        //std::cout << "host_size: " << host_size << std::endl;
    } else {
        std::cout << "env HOST_SIZE is null" << std::endl;
        exit(1);
    }
    if ((tmp_env = getenv("THREADS_NUM")) != NULL) {
        thread_num = atoi(tmp_env);
    } else {
        thread_num = THREAD_NUM;
    }
    //std::cout << "thread_num: " << thread_num << std::endl;

    char host[100] = {0};
    if (gethostname(host, sizeof(host)) < 0) {
        std::cout << "err get hostname" << std::endl;
    }

    double start_time, end_time, read_time;
    MPI_Init(&argc, &argv);

    int rank, count_ranks;
    //mpirun -n 4 hello
    //rank
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // -n
    MPI_Comm_size(MPI_COMM_WORLD, &count_ranks);
    //c++ 17
    std::hash<std::string> hash_fn;


    if (host_size != count_ranks) {
        std::cout << "host_size is not equal to ranks" << std::endl;
        exit(1);
    }
    FILE *input_file, *output_file;
    std::string input_path = std::string(argv[1]);

    char *filename = basename(argv[1]);
    std::string hosts_file = std::string(argv[3]);
    std::string gkfs_data_path = std::string(argv[4]);

    std::string pid = get_daemon_pid_by_rank(hosts_file, rank);

    std::cout << "pid :" << pid << " of rank: " << rank << std::endl;
    std::string s_output_path(argv[2]);

    std::cout << "output_path: " << s_output_path << std::endl;

    auto write_base_dir = gkfs_data_path +  "/chunks/" + filename + "/";

    input_file = fopen(argv[1], "rb");
    if (input_file == nullptr) {
        perror("open input file");
        exit(EXIT_FAILURE);
    }
    output_file = fopen(argv[2], "wb");
    if (output_file == nullptr) {
        perror("open output file");
        exit(EXIT_FAILURE);
    }
    fseek( input_file, 0, SEEK_END);
    long long file_size = ftell(input_file);

    if (truncate(argv[2], file_size) == -1) {
        perror("truncate");
        exit(EXIT_FAILURE);
    }

    //拿到metadata信息后
    long long rank_size = file_size / count_ranks;

    using namespace gkfs::utils::arithmetic;
    auto chnk_start = block_index(0,CHUNK_SIZE);
    auto  chnk_end = block_index((0+file_size) - 1, CHUNK_SIZE);
    auto total_chunks = block_count(0,file_size,CHUNK_SIZE);
    auto last_chunk_size = file_size - (total_chunks - 1) * CHUNK_SIZE;
    
    if (total_chunks == 1) {
        last_chunk_size = file_size; // the file only has one chunk
    }
    if (rank == 0) {
        printf("num of process: %d, file_size: %lld, each process_size: %lld\n", count_ranks, file_size, rank_size);
        std::cout << "total chunks: " << total_chunks << std::endl;
        std::cout << "chunk_id start: " << chnk_start << "; chunk_id end: " << chnk_end << std::endl;
        std::cout << "output file: " << output_file << std::endl;
        std::cout << "write base dir: " << write_base_dir  << std::endl;
        size_t hash = hash_fn(basename(argv[1]));
        std::cout << "Hash value of output file: " << hash << std::endl;
    }

    // Collect all chunk ids within count that have the same destination so
    // that those are send in one rpc bulk transfer
    // 收集计数内所有具有相同目标的块标识，以便它们可以在一个 RPC 批量传输中发送。
    // key rank value chunk ids
    std::map<uint64_t, std::vector<uint64_t>> target_chnks{};
    // contains the target ids, used to' access the target_chnks map.
    // First idx is chunk with potential offset
    std::vector<uint64_t> targets{};

    // targets for the first and last chunk as they need special treatment
    uint64_t chnk_start_target = 0;
    uint64_t chnk_end_target = 0;
    std::string iname="/"+std::string(basename(argv[1]));
    std::cout << "iname: " << iname << std::endl;
    for (uint64_t chnk_id = chnk_start; chnk_id <= chnk_end; chnk_id++) {
        auto target = hash_fn(iname + std::to_string(chnk_id)) % host_size;

        if (target_chnks.count(target) == 0) {
            target_chnks.insert(std::make_pair(target, std::vector<uint64_t>{chnk_id}));
            targets.push_back(target);
        } else {
            target_chnks[target].push_back(chnk_id);
        }

        // set first and last chnk targets
        if (chnk_id == chnk_start) {
            chnk_start_target = target;
        }

        if (chnk_id == chnk_end) {
            chnk_end_target = target;
        }
    }

    
    auto my_all_data_size = target_chnks[rank].size() * CHUNK_SIZE;
    if (chnk_end_target == rank) {
        // I have the last chunk
        my_all_data_size = (target_chnks[rank].size() - 1) * CHUNK_SIZE + last_chunk_size;
    }

    // std::cout << "chnk_end_target: " << chnk_start_target << " " << "chnk_end_target: " << chnk_end_target << std::endl;

    MPI_Barrier(MPI_COMM_WORLD);
    //gettimeofday(&start_time, NULL);
    start_time = MPI_Wtime();
//    if (rank == 0) {
//        for (auto i = target_chnks.begin(); i != target_chnks.end(); i++) {
//
//            std::cout << "rank: " << rank << " chunk_id: \n";
//            for (auto e: i->second) {
//                std::cout << e << " ";
//            }
//            puts("\n");
////    }
//        }
//    }

    auto my_chunks_count = target_chnks[rank].size();
    if (target_chnks.count(rank) != 0){
        //free(buffer2);
        read_time = MPI_Wtime();
        //std::cout<< "rank " << rank << "read done" << std::endl;
        // now we have all data this daemon need
        // the data is in the buffer
        // next if parallel write

        //
        if (rank != chnk_end_target){
            std::vector<uint64_t>& my_chunk_ids = target_chnks[rank];

            if (my_chunks_count >= 16 * 2) { // double cores
                thread_num = 16 * 2;
            }
            else {
                thread_num = my_chunks_count;
            }
            //fix:divide by zero
            int chunks_per_thread;
            if (thread_num != 0 ) {

                chunks_per_thread = my_chunks_count / thread_num;
            }
            // focus on the last thread
            //std::cout << "my_chunks_count " << my_chunks_count << " thread_num: " << thread_num << " chunks_per_thread: " << chunks_per_thread  << std::endl;


	        rs_time = MPI_Wtime();
            std::vector<std::thread> threads;
            for (int t = 0; t < thread_num; ++t) {
                //long long buffer_offset = t * chunks_per_thread * CHUNK_SIZE;
                long long startIndex = t * chunks_per_thread;
                long long endIndex = (t == thread_num - 1) ? (my_chunks_count - 1) : (startIndex + chunks_per_thread - 1);

                threads.emplace_back([my_chunk_ids, startIndex, endIndex, write_base_dir,output_file]() {
                    read_from_file(my_chunk_ids, startIndex, endIndex, write_base_dir,output_file);
                });
            }

            //wait for all thread done
            for (auto& thread : threads) {
                thread.join();
            }
	        re_time = MPI_Wtime();
        }
        else{
            // I have the last chunk, fwrite this chunk!
            std::vector<uint64_t>& my_chunk_ids = target_chnks[rank];
            uint64_t last_chunk_id = my_chunk_ids.back();
            my_chunk_ids.pop_back();

            my_chunks_count -= 1;
//			if (my_chunks_count == 0) {
//                goto stop;
//            }
            if (my_chunks_count >= 16 * 2) { // double cores
                thread_num = 16 * 2;
            }
            else{
                thread_num = my_chunks_count;
            }
            //fix: divide by zero
            int chunks_per_thread;
            if (thread_num != 0 ) {

                chunks_per_thread = my_chunks_count / thread_num;
            }
            // focus on the last thread
//            puts("echo count");
//            printf("RanK :%d\n", target_chnks.count(rank));

	        rs_time = MPI_Wtime();
            std::vector<std::thread> threads;
            for (int t = 0; t < thread_num; ++t) {
                long long startIndex = t * chunks_per_thread;
                long long endIndex = (t == thread_num - 1) ? (my_chunks_count - 1) : (startIndex + chunks_per_thread - 1);
                //const char* thread_data = buffer + buffer_offset;
                threads.emplace_back([my_chunk_ids, startIndex, endIndex, write_base_dir,output_file]() {
                    read_from_file(my_chunk_ids,startIndex,endIndex,write_base_dir, output_file);
                });

            }

            //wait for all thread done
            //std::cout << "!!!!!!!!!!!!!!!!!!!! to start write last, offset is " <<  (total_chunks - 1) * CHUNK_SIZE << "my rank: " << rank << std::endl;

            for (auto& thread : threads) {
                thread.join();
            }
	        re_time = MPI_Wtime();
            // I have the last chunk, fwrite this chunk!
            std::cout << "i am " << rank << std::endl;
            std::cout << "last_chunk_size: " << last_chunk_size << "last chunk offset :" << (total_chunks - 1) * CHUNK_SIZE << std::endl;
            std::string last_file = write_base_dir + std::to_string(last_chunk_id);
            std::vector<char> last_buffer(last_chunk_size, -1);
            std::ifstream file(last_file, std::ios::binary|std::ios::in);
            if (file.is_open()) {
                file.read(last_buffer.data(), last_chunk_size);

            } else {
                std::cout << "error with open " << last_file << std::endl;
            }

            // I have the last chunk, fwrite this chunk!

            fseek(output_file, (total_chunks - 1) * CHUNK_SIZE, SEEK_SET);
//            printf("lasf chnk offset : ld\n", ftell(output_file));
            fwrite(last_buffer.data(), sizeof(char), last_chunk_size, output_file);

//            printf("lasf chnk offset : ld\n", ftell(output_file));


        }


    }

    //gettimeofday(&end_time, NULL);
    end_time = MPI_Wtime();

    MPI_Barrier(MPI_COMM_WORLD);
    printf("myRank = %d hostname: %s ,time = %f read_time = %f readfile_time = %f \n",rank, host, end_time - start_time, read_time - start_time, re_time - rs_time);

    fclose(input_file);
    fclose(output_file);

    //    free(buffer);

    MPI_Finalize();

    return 0;
}
