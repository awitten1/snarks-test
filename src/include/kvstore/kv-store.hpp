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

class TxnConflict : public std::runtime_error {
public:
   using std::runtime_error::runtime_error;
};

template<typename Key, typename Value>
class DB {

  std::shared_mutex existence_mutex_;
  std::unordered_map<Key, Value> data_;
  std::atomic<uint64_t> next_txn_id_;

public:

  class Txn {
    DB* const db_;

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

    Value Get(const Key& k) {
      read_set.insert(k);
      if (copies.find(k) != copies.end()) {
        return copies[k];
      }
      return db_->Get(k);
    }

    void Commit() {
      txn_id_ = db_->GetNextTxnId();
      db_->Commit(this);
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

  uint64_t GetNextTxnId() {
    return ++next_txn_id_;
  }

  uint64_t ReadTxnIdCounter() {
    return next_txn_id_;
  }

  std::pair<Value, bool> Get(const Key& k) {
    {
      std::shared_lock<std::shared_mutex> sl(existence_mutex_);
      if (data_.find(k) != data_.end()) {
        return {data_[k], true};
      }
      return {Value{}, false};
    }
  }

  void Validate(uint64_t starttn, Txn* t) {
    {
      std::lock_guard<std::mutex> lg(validation_mutex_);
      uint64_t finishtn = next_txn_id_.load();
      auto& txn_read_set = t->GetReadSet();
      for (auto i = starttn; i < finishtn; ++i) {
        if (txns_.find(i) == txns_.end()) {
          continue;
        }
        Txn* other_txn = txns_[i];
        for (auto&& x : other_txn->GetWriteSet()) {
          if (txn_read_set.find(x.first) != txn_read_set.end()) {
            throw TxnConflict{"txn validation failure"};
          }
        }
      }
    }
  }

};