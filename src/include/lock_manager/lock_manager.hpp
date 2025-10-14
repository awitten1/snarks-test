

#include <algorithm>
#include <list>
#include <numeric>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>

template<typename Key>
class LockManager {

public:
  enum class LockMode {
    READ,
    WRITE,
  };

  class LockGrant {
    friend LockManager;
    Key k;
    typename std::list<Key>::const_iterator it;
  };

private:

  struct LockRequest {
    std::condition_variable cv;
    bool granted = false;
    LockMode mode;
  };

  struct KeyLockInfo {
    std::list<LockRequest> lock_requests;
    std::mutex m;
  };

  std::shared_mutex existence_mutex_;
  std::unordered_map<Key, KeyLockInfo> locks_;

public:
  LockGrant Lock(const Key& k, LockMode mode) {
    bool is_read = mode == LockMode::READ;

    const auto& key_info = [this, &k](){
      std::unique_lock<std::shared_mutex> lg(existence_mutex_);
      if (locks_.find(k) == locks_.end())  {
        locks_[k] = KeyLockInfo{};
      }
      return locks_[k];
    }();

    std::unique_lock<std::mutex> key_lock(key_info.m);
    auto& lock_requests = key_info.lock_requests;

    lock_requests.push_back(LockRequest{.mode = mode});
    auto& lr = lock_requests.back();

    bool all_reads = std::all_of(lock_requests.begin(), lock_requests.end(), [](const auto& lr) {
      return lr.mode == LockMode::READ;
    });

    if (all_reads && is_read) {
      lr.granted = true;
    }

    if (!is_read && lock_requests.size() == 1) {
      lr.granted = true;
    }

    LockGrant grant;
    grant.it = lock_requests.cend() - 1;
    grant.k = k;

    lr.cv.wait(key_lock, []() {
      return lr.granted;
    });

    return grant;

  }

  void Unlock(const Key& k, LockGrant grant) {
    const auto& lock_info = [this, &k]() {
      std::shared_lock sl(existence_mutex_);
      return locks_[k];
    }();
    std::lock_guard<std::mutex> lg(lock_info.m);

    auto& lock_requests = lock_info.lock_requests.
    lock_info.lock_requests.erase(grant.it);

    bool seen_write_request = false;
    for (auto&& req : lock_requests) {
      bool first = &req == &lock_requests.begin();
      if (req.mode == LockMode::WRITE) {
        if (first && !req.granted) {
          req.cv.notify_one();
        }
        break;
      } else {
        if (!req.granted) {
          req.cv.notify_one();
        }
      }
    }
  }
};
