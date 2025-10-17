#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <oneapi/tbb/concurrent_hash_map.h>
#include <algorithm>
#include <condition_variable>

#include "common/common.hpp"
#include "common/proc.hpp"

class TxnConflict : public std::runtime_error {
public:
   using std::runtime_error::runtime_error;
};

namespace DB {
template<typename DB, typename TxnCode>
void RetryLoop(DB& db, TxnCode txncode, int retries = 100, float backoff_factor = 1.5) {
  auto sleep_time = std::chrono::milliseconds(5);
  for (int i = 0; i < retries; ++i) {
    try {
      auto txn = db.Begin();
      txncode(txn);
      txn.Commit();
      break;
    } catch(const TxnConflict& e) {
      if (i == retries - 1) {
        throw;
      }
      sleep_time *= backoff_factor;
      std::this_thread::sleep_for(sleep_time);
    }
  }
}

template<typename Key, typename Value>
class DB {

  class InternalTxn {
    DB* db_;

    // txn id is assigned at commit time, just before validation.
    // Initialize it to ffff..
    uint64_t txn_id_ = -1;
    std::unordered_map<Key, Value> copies;
    std::set<Key> read_set;
    std::atomic<uint64_t> starttn_ = -1;

  public:

    InternalTxn(const InternalTxn& other) :
      db_(other.db_), txn_id_(other.txn_id_),
      copies(other.copies), read_set(other.read_set), starttn_(other.starttn_.load()) {}

    InternalTxn(InternalTxn&& other) :
      db_(other.db_), txn_id_(other.txn_id_),
      copies(std::move(other.copies)),
      read_set(std::move(other.read_set)), starttn_(other.starttn_.load()) {}

    InternalTxn(DB* db) : db_(db) {}

    std::pair<Value, bool> Get(const Key& k) {
      if (starttn_ == -1) {
        starttn_ = db_->ReadTxnIdCounter();
      }
      read_set.insert(k);
      auto it = copies.find(k);
      if (it != copies.end()) {
        return {it->second, true};
      }
      return db_->Get(k);
    }

    uint64_t GetTxnId() {
      return txn_id_;
    }

    const std::set<Key>& GetReadSet() {
      return read_set;
    }

    uint64_t GetStartTn() {
      return starttn_.load();
    }

    const std::unordered_map<Key, Value>& GetWriteSet() const {
      return copies;
    }


    DB* GetDB() {
      return db_;
    }

    void SetTxnId(uint64_t txn_id) {
      txn_id_ = txn_id;
    }


    void Put(const Key& k, const Value& v) {
      if (starttn_ == -1) {
        starttn_ = db_->ReadTxnIdCounter();
      }
      copies[k] = v;
    }

  };

public:

  class Txn {
    std::optional<typename std::list<InternalTxn>::iterator> it_;
    bool committed_attempted_ = false;
    DB<Key, Value>* db_;

  public:

    Txn(DB<Key, Value>* db) : db_(db) {}

    std::pair<Value, bool> Get(const Key& k) {
      if (!it_.has_value()) {
        it_.emplace(db_->StartTxn());
      }
      return (*it_)->Get(k);
    }

    void Commit() {
      if (!it_.has_value()) {
        return;
      }
      OnBlockExit obe([this]() {
        committed_attempted_ = true;
      });
      if (!committed_attempted_) {
        (*it_)->GetDB()->Commit(*it_);
      }
    }

    void Put(const Key& k, const Value& v) {
      if (!it_.has_value()) {
        it_.emplace(db_->StartTxn());
      }
      (*it_)->Put(k, v);
    }

  };

  ~DB() {
    {
      std::unique_lock<std::mutex> lg(shutdown_mutex_);
      shutdown_ = true;
      cv_.notify_all();
    }
    gc_thread_.join();
    stats_thread_.join();
  }

  Txn Begin() {
    return Txn{this};
  }

  void DumpValues() {
    for (auto it = data_.begin(); it != data_.end(); ++it) {
      std::cout << '(' << it->first << ", " << it->second << ')';
    }
    std::cout << std::endl << std::endl;
  }

  DB() {
    gc_thread_ = std::thread([this]() {
      for (;;) {
        {
          std::unique_lock<std::mutex> lg(shutdown_mutex_);
          cv_.wait_for(lg, std::chrono::milliseconds(100), [this]() {
            return shutdown_;
          });
          if (shutdown_) {
            return;
          }
        }

        uint64_t min_start_tn = -1;
        {
          std::lock_guard<std::mutex> lg(ongoing_txns_mutex_);
          for (auto& itxn : ongoing_txns_) {
            if (itxn.GetStartTn() < min_start_tn) {
              min_start_tn = itxn.GetStartTn();
            }
          }
        }

        {
          std::lock_guard<std::mutex> lg(validation_mutex_);
          for (auto it = committed_txns_.begin(); it != committed_txns_.end();) {
            if (it->first >= min_start_tn) {
              break;
            }
            it = committed_txns_.erase(it);
            ++pruned_txns_;
          }
        }
      }
    });

    stats_thread_ = std::thread([this]() {
      auto log_stats = [=]() {
        ProcMetrics metrics = read_proc_pid_status();
        size_t map_size;
        {
          std::lock_guard<std::mutex> lg(validation_mutex_);
          map_size = committed_txns_.size();
        }

        size_t num_ongoing_txns;
        {
          std::lock_guard<std::mutex> lg(ongoing_txns_mutex_);
          num_ongoing_txns = ongoing_txns_.size();
        }

        size_t num_keys = data_.size();
        std::cerr << '{' << " vmsize_gb: " << metrics.vmsize
          << ", rssanon_gb: " << metrics.rssanon << ", committed_txns_size: " << map_size
          << ", num_keys: " << num_keys << ", pruned_txns: " << pruned_txns_
          << ", num_ongoing_txns: " << num_ongoing_txns
          << ", txn_aborts: " << txn_aborts_.load()
          << ", committed_txn_count: " << committed_txn_count_.load() << " }" << std::endl;
      };
      for (;;) {
        {
          std::unique_lock<std::mutex> lg(shutdown_mutex_);
          cv_.wait_for(lg, std::chrono::seconds(1), [this]() {
            return shutdown_;
          });
          log_stats();
          if (shutdown_) {
            return;
          }
        }

      }
    });
  }

private:

  // The actual data.
  tbb::concurrent_hash_map<Key, Value> data_;

  // Transactions that have committed but must be preserved
  // in order to validate ongoing transactions.
  std::mutex validation_mutex_;
  std::map<uint64_t, InternalTxn> committed_txns_;
  // The next txn id to dispense.
  std::atomic<uint64_t> next_txn_id_ = 0;

  // background thread to cleanup committed_txns_ map.
  std::thread gc_thread_;

  // background thread to log metrics.
  std::thread stats_thread_;
  std::atomic<uint64_t> pruned_txns_ = 0;
  std::atomic<uint64_t> committed_txn_count_ = 0;
  std::atomic<uint64_t> txn_aborts_ = 0;

  // Outstanding transactions.
  std::mutex ongoing_txns_mutex_;
  std::list<InternalTxn> ongoing_txns_;

  std::mutex shutdown_mutex_;
  std::condition_variable cv_;
  bool shutdown_ = false;

  uint64_t ReadTxnIdCounter() {
    return next_txn_id_;
  }

  friend Txn;
  typename std::list<InternalTxn>::iterator StartTxn() {
    std::lock_guard<std::mutex> lg(ongoing_txns_mutex_);
    ongoing_txns_.push_back(InternalTxn{this});
    auto end = ongoing_txns_.end();
    end--;
    return end;
  }

  std::pair<Value, bool> Get(const Key& k) {
    {
      typename decltype(data_)::const_accessor a;
      bool found = data_.find(a, k);
      if (found) {
        return {a->second, true};
      }
      return {Value{}, false};
    }
  }

  void WritePhase(std::lock_guard<std::mutex>& validation_mutex_, typename std::list<InternalTxn>::iterator t) {
    for (const auto& [k, v] : t->GetWriteSet()) {
      typename decltype(data_)::accessor a;
      data_.insert(a, k);
      a->second = v;
    }
  }

  void Commit(typename std::list<InternalTxn>::iterator ongoing_txn) {
    Validate(ongoing_txn);
    ++committed_txn_count_;
  }

  void Validate(typename std::list<InternalTxn>::iterator ongoing_txn) {
    {
      std::lock_guard<std::mutex> lg(validation_mutex_);
      uint64_t starttn = ongoing_txn->GetStartTn();
      if (starttn == -1) {
        return;
      }
      uint64_t finishtn = next_txn_id_.load();
      auto& txn_read_set = ongoing_txn->GetReadSet();
      bool valid = true;

      for (auto i = starttn; i <= finishtn; ++i) {
        auto it = committed_txns_.find(i);
        if (it == committed_txns_.end()) {
          continue;
        }
        const InternalTxn& other_txn = it->second;
        for (auto&& x : other_txn.GetWriteSet()) {
          if (txn_read_set.find(x.first) != txn_read_set.end()) {
            valid = false;
            {
              std::lock_guard<std::mutex> lg(ongoing_txns_mutex_);
              ongoing_txns_.erase(ongoing_txn);
            }
            ++txn_aborts_;
            throw TxnConflict{"txn validation failure"};
          }
        }
      }
      WritePhase(lg, ongoing_txn);
      ongoing_txn->SetTxnId(next_txn_id_++);
      {
        std::lock_guard<std::mutex> lg(ongoing_txns_mutex_);
        uint64_t txn_id = ongoing_txn->GetTxnId();
        committed_txns_.emplace(txn_id, std::move(*ongoing_txn));
        ongoing_txns_.erase(ongoing_txn);
      }

    }
  }

};

}