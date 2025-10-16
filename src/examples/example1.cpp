#include "kvstore/kv-store.hpp"
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ratio>
#include <thread>

#include <random>
#include <string>
#include <iostream>


thread_local std::mt19937 gen{std::random_device{}()};
thread_local std::uniform_int_distribution<int> dist(0, 255);
thread_local std::uniform_int_distribution<int> dist10000(0, 10000);

std::string rand_string() {
  std::string ret;

  for (int i = 0; i < 10; ++i) {
    ret += static_cast<char>(dist(gen));
  }

  return ret;
}

template<typename DB, typename Txn>
void RetryLoop(DB& db, Txn txncode, int retries = 1000, float backoff_factor = 1.5) {
  auto sleep_time = std::chrono::milliseconds(5);
  for (int i = 0; i < retries; ++i) {
    try {
      auto txn = db.Begin();
      txncode(txn);
    } catch(const TxnConflict&) {
      std::this_thread::sleep_for(sleep_time);
      sleep_time *= backoff_factor;
    }
  }
}

int main() {
  DB<int64_t, std::string> db;

  std::vector<std::thread> threads;
  OnBlockExit obe([&threads]() {
    for (auto& thread : threads) {
      thread.join();
    }
  });

  int num_threads = 5;

  for (int i = 0; i < num_threads; ++i) {
    std::thread t([&db]() {
      int num_transactions = 10000;
      for (int j = 0; j < num_transactions; ++j) {
        RetryLoop(db, [](auto& txn) {
          int64_t key = dist(gen);
          int num_keys = 3;
          for (int k = 0; k < num_keys; ++k) {
            std::string val1 = rand_string();
            txn.Put(key, val1);
            const auto [val, found] = txn.Get(key);
            assert(found);
            assert(val == val1);
          }
        });
      }
    });
    threads.push_back(std::move(t));
  }

}