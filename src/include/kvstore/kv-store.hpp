#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "common/common.hpp"

class TxnConflict : public std::runtime_error {
public:
   using std::runtime_error::runtime_error;
};

template<typename Key, typename Value>
class DB {

  std::shared_mutex data_mutex_;
  std::unordered_map<Key, Value> data_;
  std::atomic<uint64_t> next_txn_id_;

public:

  class Txn {
    DB* db_;

    // txn id is assigned at commit time, just before validation.
    // Initialize it to ffff..
    uint64_t txn_id_ = -1;
    std::unordered_map<Key, Value> copies;
    std::set<Key> read_set;
    bool committed_ = false;
    uint64_t starttn_;


    void Validate() {
      db_->Validate(starttn_, this);
    }

  public:
    Txn(DB* db) : db_(db), starttn_(db_->ReadTxnIdCounter()) {}

    ~Txn() {
      if (!committed_)
        Commit();
    }

    const std::set<Key>& GetReadSet() {
      return read_set;
    }

    const std::unordered_map<Key, Value>& GetWriteSet() {
      return copies;
    }

    std::pair<Value, bool> Get(const Key& k) {
      read_set.insert(k);
      if (copies.find(k) != copies.end()) {
        return {copies[k], true};
      }
      return db_->Get(k);
    }

    void Commit() {
      db_->Validate(starttn_, this);
    }

    void SetTxnId(uint64_t txn_id) {
      txn_id_ = txn_id;
    }


    void Put(const Key& k, const Value& v) {
      copies[k] = v;
    }
  };

  Txn Begin() {
    return Txn{this};
  }

private:

  std::mutex validation_mutex_;
  std::map<uint64_t, Txn*> txns_;

  friend Txn;

  uint64_t ReadTxnIdCounter() {
    return next_txn_id_;
  }

  std::pair<Value, bool> Get(const Key& k) {
    {
      std::shared_lock<std::shared_mutex> sl(data_mutex_);
      if (data_.find(k) != data_.end()) {
        return {data_[k], true};
      }
      return {Value{}, false};
    }
  }


  void WritePhase(std::lock_guard<std::mutex>&, Txn* t) {
    for (const auto& [k, v] : t->GetWriteSet()) {
      std::unique_lock<std::shared_mutex> lg(data_mutex_);
      data_[k] = v;
    }
  }

  void Validate(uint64_t starttn, Txn* t) {
    {
      std::lock_guard<std::mutex> lg(validation_mutex_);
      uint64_t finishtn = next_txn_id_.load();
      auto& txn_read_set = t->GetReadSet();
      bool valid = true;

      OnBlockExit obe([&valid, t, this]() {
        if (!valid) {
          *t = Begin();
        }
      });

      for (auto i = starttn; i < finishtn; ++i) {
        if (txns_.find(i) == txns_.end()) {
          continue;
        }
        Txn* other_txn = txns_[i];
        for (auto&& x : other_txn->GetWriteSet()) {
          if (txn_read_set.find(x.first) != txn_read_set.end()) {
            valid = false;
            throw TxnConflict{"txn validation failure"};
          }
        }
      }
      WritePhase(lg, t);
      t->SetTxnId(finishtn + 1);

    }
  }

};