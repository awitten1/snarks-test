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

  class InternalTxn {
    DB* db_;

    // txn id is assigned at commit time, just before validation.
    // Initialize it to ffff..
    uint64_t txn_id_ = -1;
    std::unordered_map<Key, Value> copies;
    std::set<Key> read_set;
    uint64_t starttn_;

  public:

    InternalTxn(DB* db) : db_(db), starttn_(db_->ReadTxnIdCounter()) {}

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

    uint64_t GetStartTn() {
      return starttn_;
    }
  };

public:

  class Txn {
    typename std::list<InternalTxn>::iterator it_;
    bool committed = false;

  public:
    Txn(typename std::list<InternalTxn>::iterator it) : it_(it) {}

    std::pair<Value, bool> Get(const Key& k) {
      return it_->Get(k);
    }

    void Commit() {
      if (!committed) {
        it_->GetDB()->Commit(it_);
        committed = true;
      }
    }

    void SetTxnId(uint64_t txn_id) {
      it_->SetTxnId(txn_id);
    }

    void Put(const Key& k, const Value& v) {
      it_->Put(k, v);
    }

    ~Txn() {
      Commit();
    }
  };

  Txn Begin() {
    std::lock_guard<std::mutex> lg(ongoing_txns_mutex_);
    ongoing_txns_.push_back(InternalTxn{this});
    auto end = ongoing_txns_.end();
    end--;
    return Txn{end};
  }

private:

  // The actual data.  Get shared lock for reads, exclusive for writes.
  std::shared_mutex data_mutex_;
  std::unordered_map<Key, Value> data_;

  // Transactions that have committed but must be preserved
  // in order to validate ongoing transactions.
  std::mutex validation_mutex_;
  std::map<uint64_t, InternalTxn> committed_txns_;
  // The next txn id to dispense.
  std::atomic<uint64_t> next_txn_id_ = 0;

  // Outstanding transactions.
  std::mutex ongoing_txns_mutex_;
  std::list<InternalTxn> ongoing_txns_;

  friend InternalTxn;

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


  void WritePhase(std::lock_guard<std::mutex>& validation_mutex_, typename std::list<InternalTxn>::iterator t) {
    for (const auto& [k, v] : t->GetWriteSet()) {
      std::unique_lock<std::shared_mutex> lg(data_mutex_);
      data_[k] = v;
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

      OnBlockExit obe([&valid, ongoing_txn, this]() {
        if (!valid) {
          *ongoing_txn = InternalTxn{this};
        }
      });

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
      ongoing_txn->SetTxnId(++next_txn_id_);
      {
        std::lock_guard<std::mutex> lg(ongoing_txns_mutex_);
        uint64_t txn_id = ongoing_txn->GetTxnId();
        committed_txns_.emplace(txn_id, std::move(*ongoing_txn));
        ongoing_txns_.erase(ongoing_txn);
      }

    }
  }

};