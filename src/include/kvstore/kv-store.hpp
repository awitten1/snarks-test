#pragma once

#include <cassert>
#include <unordered_map>

template<typename Key, typename Value>
class DB {
  std::unordered_map<Key, Value> data_;

public:
  class Txn {
    DB* const db_;
  public:
    Txn(DB* db) : db_(db) {}
    ~Txn();

    Value Get(const Key& k) {
      return db_->Get(k);
    }

    void Put(const Key& k, const Value& v) {
      db_->Put(k, v);
    }
  };

  Txn Begin();
  Value Get(const Key&);
  void Put(const Key&, const Value&);
};