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
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
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

template<typename Key, typename Value>
class DB {

  class InternalTxn {
    DB* db_;

    // txn id is assigned at commit time, just before validation.
    // Initialize it to ffff..
    uint64_t txn_id_ = -1;
    std::unordered_map<Key, Value> copies;
    std::set<Key> read_set;
    uint64_t starttn_;

  public:

    InternalTxn(DB* db, uint64_t starttn) : db_(db), starttn_(starttn) {}

    std::pair<Value, bool> Get(const Key& k) {
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
      return starttn_;
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
      copies[k] = v;
    }

  };

public:

  class Txn {
    typename std::list<InternalTxn>::iterator it_;
    bool committed_attempted_ = false;

  public:
    Txn(typename std::list<InternalTxn>::iterator it) : it_(it) {}

    std::pair<Value, bool> Get(const Key& k) {
      return it_->Get(k);
    }

    void Commit() {
      OnBlockExit obe([this]() {
        committed_attempted_ = true;
      });
      if (!committed_attempted_) {
        it_->GetDB()->Commit(it_);
      }
    }

    void SetTxnId(uint64_t txn_id) {
      it_->SetTxnId(txn_id);
    }

    void Put(const Key& k, const Value& v) {
      it_->Put(k, v);
    }

  };

  Txn Begin() {
    std::lock_guard<std::mutex> lg(ongoing_txns_mutex_);
    ongoing_txns_.push_back(InternalTxn{this, ReadTxnIdCounter()});
    auto end = ongoing_txns_.end();
    end--;
    return Txn{end};
  }

  ~DB() {
    {
      std::unique_lock<std::mutex> lg(shutdown_mutex_);
      shutdown_ = true;
      cv_.notify_all();
    }
    gc_thread_.join();
    stats_thread_.join();
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
          }
        }
      }
    });

    stats_thread_ = std::thread([this]() {
      for (;;) {
        {
          std::unique_lock<std::mutex> lg(shutdown_mutex_);
          cv_.wait_for(lg, std::chrono::seconds(1), [this]() {
            return shutdown_;
          });
          if (shutdown_) {
            return;
          }
        }
        ProcMetrics metrics = read_proc_pid_status();

        size_t map_size;
        {
          std::lock_guard<std::mutex> lg(validation_mutex_);
          map_size = committed_txns_.size();
        }

        size_t num_keys = data_.size();
        std::cerr << '{' << " vmsize: " << metrics.vmsize << ","
          << " rssanon: " << metrics.rssanon << ", committed_txns_size: " << map_size
          << ", num_keys: " << num_keys << " }" << std::endl;
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

  // Outstanding transactions.
  std::mutex ongoing_txns_mutex_;
  std::list<InternalTxn> ongoing_txns_;

  std::mutex shutdown_mutex_;
  std::condition_variable cv_;
  bool shutdown_ = false;

  uint64_t ReadTxnIdCounter() {
    return next_txn_id_;
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
  }

  void Validate(typename std::list<InternalTxn>::iterator ongoing_txn) {
    {
      std::lock_guard<std::mutex> lg(validation_mutex_);
      uint64_t starttn = ongoing_txn->GetStartTn();
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