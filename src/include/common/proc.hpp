

#include <fstream>
#include <sstream>
#include <unordered_map>

struct ProcMetrics {
  long vmsize;
  long rssanon;
  long rssfile;
};

inline ProcMetrics read_proc_pid_status() {
  std::ifstream f("/proc/self/status");

  ProcMetrics ret;
  std::unordered_map<std::string, long*> key_to_ptr = {
    {"VmRSS", &ret.vmsize},
    {"RssAnon", &ret.rssanon},
    {"RssFile", &ret.rssfile}
  };

  for (std::string line; std::getline(f, line);) {
    std::istringstream iss(line);

    std::string key, value;
    iss >> key;
    iss >> value;
    if (key_to_ptr.find(key) != key_to_ptr.end())
      *key_to_ptr[key] = std::stoll(value);
  }

  return ret;
}