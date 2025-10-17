

#include <fstream>
#include <iostream>
#include <sstream>
#include <unordered_map>

struct ProcMetrics {
  float vmsize = -1;
  float rssanon = -1;
  float rssfile = -1;
};

inline ProcMetrics read_proc_pid_status() {
  std::ifstream f("/proc/self/status");

  ProcMetrics ret;
  std::unordered_map<std::string, float*> key_to_ptr = {
    {"VmSize:", &ret.vmsize},
    {"RssAnon:", &ret.rssanon},
    {"RssFile:", &ret.rssfile}
  };

  for (std::string line; std::getline(f, line);) {
    std::istringstream iss(line);

    std::string key, value;

    iss >> key;
    iss >> value;

    if (key_to_ptr.find(key) != key_to_ptr.end()) {
      *key_to_ptr[key] = (float)std::stoll(value)/1e6;
    }
  }

  return ret;
}