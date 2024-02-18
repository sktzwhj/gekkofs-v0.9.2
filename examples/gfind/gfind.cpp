/*
  Copyright 2018-2024, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2024, Johannes Gutenberg Universitaet Mainz, Germany

  This software was partially supported by the
  EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).

  This software was partially supported by the
  ADA-FS project under the SPPEXA project funded by the DFG.

  This file is part of GekkoFS.

  GekkoFS is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  GekkoFS is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with GekkoFS.  If not, see <https://www.gnu.org/licenses/>.

  SPDX-License-Identifier: GPL-3.0-or-later
*/

/* Based on pfind from ior500 */
/* https://github.com/VI4IO/pfind/ */

#include <cmath>
#include <cstring>
#include <getopt.h>
#include <iostream>
#include <mpi.h>
#include <queue>
#include <regex.h>
#include <stdio.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits>
#include <cstdint>

using namespace std;

/* Minimal struct needed for io500 find */
/* We could also do the filtering on the server */
struct dirent_extended {
  size_t size;
  time_t ctime;
  unsigned short d_reclen;
  unsigned char d_type;
  char d_name[1];
};

/* Function exported from GekkoFS LD_PRELOAD, code needs to be compiled with
 * -fPIC, if not will segfault */
extern "C" int gkfs_getsingleserverdir(const char *path,
                                       struct dirent_extended *dirp,
                                       unsigned int count, int server)
    __attribute__((weak));

/* PFIND OPTIONS EXTENDED We need to add the GekkoFS mount dir and the number of
 * servers */
typedef struct {
  char * workdir;
  int just_count;
  int print_by_process;
  char *results_dir;
  int stonewall_timer;
  int print_rates;

  char *timestamp_file;
  char *name_pattern;
  regex_t name_regex;
  uint64_t size;

  int num_servers;
  char *mountdir;
  // optimizing parameters NOT USED
  int queue_length;
  int max_entries_per_iter;
  int steal_from_next;            // if true, then steal from the next process
  int parallel_single_dir_access; // if 1, use hashing to parallelize single
                                  // directory access, if 2 sequential increment

  int verbosity;
} pfind_options_t;

typedef struct {
  uint64_t ctime_min;
  double stonewall_endtime;
  FILE *logfile;
  int needs_stat;
} pfind_runtime_options_t;

static pfind_runtime_options_t runtime;

int pfind_rank;

static pfind_options_t *opt;

void pfind_abort(const string str) {
  printf("%s", str.c_str());
  exit(1);
}

static void pfind_print_help(pfind_options_t *res) {
  printf("pfind \nSynopsis:\n"
         "pfind <workdir> [-newer <timestamp file>] [-size <size>c] [-name "
         "<substr>] [-regex <regex>] [-S <numserver>] [-M <mountdir>]\n"
         "\tworkdir = \"%s\"\n"
         "\t-newer = \"%s\"\n"
         "\t-name|-regex = \"%s\"\n"
         "\t-S: num servers = \"%d\"\n"
         "\t-M: mountdir = \"%s\"\n"
         "Optional flags\n"
         "\t-h: prints the help\n"
         "\t--help: prints the help without initializing MPI\n",
         res->workdir, res->timestamp_file, res->name_pattern, res->num_servers,
         res->mountdir);
}
MPI_Comm pfind_com;
int pfind_size;
pfind_options_t *pfind_parse_args(int argc, char **argv, int force_print_help,
                                  MPI_Comm com) {
  MPI_Comm_rank(com, &pfind_rank);
  MPI_Comm_size(com, &pfind_size);
  pfind_com = com;

  pfind_options_t *res = (pfind_options_t *)malloc(sizeof(pfind_options_t));
  memset(res, 0, sizeof(pfind_options_t));
  int print_help = force_print_help;

  res->workdir = nullptr;
  res->results_dir = nullptr;
  res->verbosity = 0;
  res->timestamp_file = nullptr;
  res->name_pattern = nullptr;
  res->size = std::numeric_limits<uint64_t>::max();
  res->queue_length = 100000;
  res->max_entries_per_iter = 1000;
  char *firstarg = nullptr;

  // when we find special args, we process them
  // but we need to replace them with 0 so that getopt will ignore them
  // and getopt will continue to process beyond them
  for (auto i = 1; i < argc - 1; i++) {
    if (strcmp(argv[i], "-newer") == 0) {
      res->timestamp_file = strdup(argv[i + 1]);
      argv[i][0] = 0;
      argv[++i][0] = 0;
    } else if (strcmp(argv[i], "-size") == 0) {
      char *str = argv[i + 1];
      char extension = str[strlen(str) - 1];
      str[strlen(str) - 1] = 0;
      res->size = atoll(str);
      switch (extension) {
      case 'c':
        break;
      default:
        pfind_abort("Unsupported exension for -size\n");
      }
      argv[i][0] = 0;
      argv[++i][0] = 0;
    } else if (strcmp(argv[i], "-name") == 0) {
      res->name_pattern = (char *)malloc(strlen(argv[i + 1]) * 4 + 100);
      // transform a traditional name pattern to a regex:
      char *str = argv[i + 1];
      char *out = res->name_pattern;
      int pos = 0;
      for (unsigned i = 0; i < strlen(str); i++) {
        if (str[i] == '*') {
          pos += sprintf(out + pos, ".*");
        } else if (str[i] == '.') {
          pos += sprintf(out + pos, "[.]");
        } else if (str[i] == '"' || str[i] == '\"') {
          // erase the "
        } else {
          out[pos] = str[i];
          pos++;
        }
      }
      out[pos] = 0;

      auto ret = regcomp(&res->name_regex, res->name_pattern, 0);
      if (ret) {
        pfind_abort("Invalid regex for name given\n");
      }
      argv[i][0] = 0;
      argv[++i][0] = 0;
    } else if (strcmp(argv[i], "-regex") == 0) {
      res->name_pattern = strdup(argv[i + 1]);
      auto ret = regcomp(&res->name_regex, res->name_pattern, 0);
      if (ret) {
        pfind_abort("Invalid regex for name given\n");
      }
      argv[i][0] = 0;
      argv[++i][0] = 0;
    } else if (!firstarg) {
      firstarg = strdup(argv[i]);
      argv[i][0] = 0;
    }
  }
  if (argc == 2) {
    firstarg = strdup(argv[1]);
  }

  int c;
  while ((c = getopt(argc, argv, "CPs:r:vhD:xq:H:NM:S:")) != -1) {
    if (c == -1) {
      break;
    }

    switch (c) {
    case 'H':
      res->parallel_single_dir_access = atoi(optarg);
      break;
    case 'N':
      res->steal_from_next = 1;
      break;
    case 'x':
      /* ignore fake arg that we added when we processed the extra args */
      break;
    case 'P':
      res->print_by_process = 1;
      break;
    case 'C':
      res->just_count = 1;
      break;
    case 'D':
      if (strcmp(optarg, "rates") == 0) {
        res->print_rates = 1;
      } else {
        pfind_abort("Unsupported debug flag\n");
      }
      break;
    case 'h':
      print_help = 1;
      break;
    case 'r':
      res->results_dir = strdup(optarg);
      break;
    case 'q':
      res->queue_length = atoi(optarg);
      break;
      if (res->queue_length < 10) {
        pfind_abort("Queue must be at least 10 elements!\n");
      }
      break;
    case 's':
      res->stonewall_timer = atol(optarg);
      break;
    case 'S':
      res->num_servers = atoi(optarg);
      break;
    case 'M':
      res->mountdir = strdup(optarg);
      break;
    case 'v':
      res->verbosity++;
      break;
    case 0:
      break;
    }
  }
  if (res->verbosity > 2 && pfind_rank == 0) {
    printf("Regex: %s\n", res->name_pattern);
  }

  if (print_help) {
    if (pfind_rank == 0)
      pfind_print_help(res);
    int init;
    MPI_Initialized(&init);
    if (init) {
      MPI_Finalize();
    }
    exit(0);
  }

  if (!firstarg) {
    pfind_abort("Error: pfind <directory>\n");
  }
  res->workdir = firstarg;

  return res;
}

/* Master send a new path to the workers */
void send_newPath(string path) {
  auto count = path.size() + 1;
  MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast((void *)path.c_str(), count, MPI_CHAR, 0, MPI_COMM_WORLD);
}

/* Clients get a new path, getting a "0" size char means there is no new path*/
string recv_newPath() {
  int count;
  MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);
  if (count == 0)
    return "Terminate";
  char buf[count];
  MPI_Bcast(buf, count, MPI_CHAR, 0, MPI_COMM_WORLD);
  return buf;
}

/* Client Processing a path.
 * We increment local checked/found based on the filters
 * Each client sends the request to a subset of GekkoFS servers.
 * We use 102400 (plus space from 255 chars paths) so it is nearly 1M files per
 * server, which is enough for most cases
 *
 */
void dirProcess(const string path, unsigned long long &checked,
                unsigned long long &found, queue<string> &dirs,
                unsigned int world_rank, unsigned int world_size,
                pfind_options_t *opt) {
  struct dirent_extended *getdir = (struct dirent_extended *)malloc(
      (sizeof(struct dirent_extended) + 255) * 1024 * 100);
  memset(getdir, 0, (sizeof(struct dirent_extended) + 255) * 1024 * 100);
  // cout << "PROCESSING " << world_rank << "/"<< world_size << " = " << path <<
  // endl;

  
  int servers_per_node = ceil(opt->num_servers / (world_size - 1));
  if (servers_per_node == 0)
     servers_per_node++;
  for (int it = 0; it < servers_per_node; it++) {
      auto server = (world_rank - 1) * servers_per_node + it;
      if (server >= opt->num_servers)
        break;

      unsigned long long total_size = 0;
      auto n = gkfs_getsingleserverdir(
          path.c_str(), getdir,
          (sizeof(struct dirent_extended) + 255) * 1024 * 100, server);
      struct dirent_extended *temp = getdir;

      while (total_size < n) {
        if (strlen(temp->d_name) == 0)
          break;
        total_size += temp->d_reclen;
        /* Queue directory to process */
        if (temp->d_type == 1) {
          string slash;
          if (path[path.size() - 1] != '/')
            slash = "/";
          checked++;
          dirs.push(path + slash + temp->d_name);
          temp = reinterpret_cast<dirent_extended *>(reinterpret_cast<char *>(temp) +
                                                     temp->d_reclen);
          continue;
        }
        /* Find filtering */
        auto timeOK = true;
        if (opt->timestamp_file) {
          if ((uint64_t)temp->ctime < runtime.ctime_min)
            timeOK = false;
        }
        if (timeOK and (temp->size == opt->size or opt->size == std::numeric_limits<uint64_t>::max()))
          if (!(opt->name_pattern &&
                regexec(&opt->name_regex, temp->d_name, 0, nullptr, 0)))
            found++;
        checked++;
        temp =
            reinterpret_cast<dirent_extended *>(reinterpret_cast<char *>(temp) + temp->d_reclen);
       }
  }
}

int process(char *processor_name, int world_rank, int world_size,
            pfind_options_t *opt) {
  // Print off a hello world message

  // INIT PFIND
  memset(&runtime, 0, sizeof(pfind_runtime_options_t));
  int ret;
  /* Get timestamp file */
  if (opt->timestamp_file) {
    if (pfind_rank == 0) {
      static struct stat timer_file{};
      if (lstat(opt->timestamp_file, &timer_file) != 0) {
        printf("Could not open: \"%s\", error: %s", opt->timestamp_file,
               strerror(errno));
        pfind_abort("\n");
      }
      runtime.ctime_min = timer_file.st_ctime;
    }
    MPI_Bcast(&runtime.ctime_min, 1, MPI_INT, 0, pfind_com);
  }

  auto iterations = 0;
  if (world_rank == 0) {
    queue<string> dirs;
    string workdir = opt->workdir;
    workdir = workdir.substr(strlen(opt->mountdir), workdir.size());
    if (workdir.size() == 0)
      workdir = "/";
    dirs.push(workdir);

    do {

      string processpath = dirs.front();
      dirs.pop();
      // DISTRIBUTE WORK
      send_newPath(processpath);
      auto received_strings = true;
      // We need to gather new directories found (we use send-recv)
      for (auto i = 1; i < world_size; i++) {
        received_strings = true;
        while (received_strings) {
          received_strings = false;
          //	cout << " Checking from " << i << endl;
          MPI_Status mpistatus;
          MPI_Probe(i, 0, MPI_COMM_WORLD, &mpistatus);
          int count;
          MPI_Get_count(&mpistatus, MPI_CHAR, &count);
          char buf[count];
          MPI_Recv(&buf, count, MPI_CHAR, i, 0, MPI_COMM_WORLD, &mpistatus);
          if (count == 0) {
            continue;
          }
          // cout << " Receiving from " << i << " ---- " << buf << endl;
          string s = buf;
          dirs.push(s);
          received_strings = true;
        }
      }
      // cout << "NO more paths " << dirs.size() << endl;
    } while (!dirs.empty());

    auto count = 0;
    MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);

    unsigned long long *Array_checked =
        (unsigned long long *)malloc(sizeof(unsigned long long) * world_size);
    unsigned long long *Array_found =
        (unsigned long long *)malloc(sizeof(unsigned long long) * world_size);
    unsigned long long checked = 0;
    unsigned long long found = 0;

    MPI_Gather(&checked, 1, MPI_UNSIGNED_LONG_LONG, Array_checked, 1,
               MPI_UNSIGNED_LONG_LONG, 0, MPI_COMM_WORLD);
    MPI_Gather(&found, 1, MPI_UNSIGNED_LONG_LONG, Array_found, 1,
               MPI_UNSIGNED_LONG_LONG, 0, MPI_COMM_WORLD);

    for (int i = 0; i < world_size; i++) {
      checked += Array_checked[i];
      found += Array_found[i];
    }

    cout << "MATCHED " << found << "/" << checked << endl;
  }

  else {
    unsigned long long checked = 0;
    unsigned long long found = 0;
    while (1) {

      string toProcess = recv_newPath();
      if (toProcess == "Terminate") {
        break;
      }
      // cout << "REceived " << toProcess << " --- " << world_rank << endl;
      queue<string> dirs;

      dirProcess(toProcess, checked, found, dirs, world_rank, world_size, opt);
      // Send NEW DIRS to master
      while (!dirs.empty()) {
        string s = dirs.front();
        dirs.pop();
        // cout << world_rank << " --> Sending " << s << endl;
        MPI_Send((void *)s.c_str(), (s.size() + 1), MPI_CHAR, 0, 0,
                 MPI_COMM_WORLD);
      }
      // cout << world_rank << " --> Sending 0 " << endl;
      MPI_Send((void *)0, 0, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Gather(&checked, 1, MPI_UNSIGNED_LONG_LONG, nullptr, 1,
               MPI_UNSIGNED_LONG_LONG, 0, MPI_COMM_WORLD);
    MPI_Gather(&found, 1, MPI_UNSIGNED_LONG_LONG, nullptr, 1,
               MPI_UNSIGNED_LONG_LONG, 0, MPI_COMM_WORLD);
  }

  return 0;
}

int main(int argc, char **argv) {

  for (int i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--help") == 0) {
      argv[i][0] = 0;
      pfind_rank = 0;
      pfind_parse_args(argc, argv, 1, MPI_COMM_SELF);
      exit(0);
    }
  }

  // Initialize the MPI environment
  MPI_Init(&argc, &argv);

  // Get the number of processes
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // Get the rank of the process
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  opt = pfind_parse_args(argc, argv, 0, MPI_COMM_WORLD);
  //	cout << opt->num_servers << " -- " << opt->mountdir << endl;
  // Get the name of the processor
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Get_processor_name(processor_name, &name_len);

  process(processor_name, world_rank, world_size, opt);

  // Finalize the MPI environment.
  MPI_Finalize();
}
