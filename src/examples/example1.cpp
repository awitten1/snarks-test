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
thread_local std::uniform_int_distribution<int> dist(0, 100000);
thread_local std::uniform_int_distribution<int> dist10000(0, 10000);

std::string rand_string() {
  std::string ret;

  for (int i = 0; i < 10; ++i) {
    ret += static_cast<char>(dist(gen));
  }

  return ret;
}

std::atomic<uint64_t> commits = 0;

template<typename DB, typename TxnCode>
void RetryLoop(DB& db, TxnCode txncode, int retries = 10, float backoff_factor = 1.5) {
  auto sleep_time = std::chrono::milliseconds(5);
  for (int i = 0; i < retries; ++i) {
    try {
      auto txn = db.Begin();
      txncode(txn);
      txn.Commit();
      ++commits;
      break;
    } catch(const TxnConflict& e) {
      std::cout << "conflict" << std::endl;
      sleep_time *= backoff_factor;
      std::this_thread::sleep_for(sleep_time);
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
    std::cout << "committed " << commits << " txns" << std::endl;
  });

  int num_threads = 5;

  for (int i = 0; i < num_threads; ++i) {
    std::thread t([&db]() {
      int num_transactions = 1000;
      for (int j = 0; j < num_transactions; ++j) {
        RetryLoop(db, [](auto& txn) {
          int64_t key = dist(gen);
          int num_keys = 1;
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

  return 0;

}