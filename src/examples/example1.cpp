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

int main() {
  DB<int64_t, std::string> db;

  std::vector<std::thread> threads;
  OnBlockExit obe([&threads]() {
    for (auto& thread : threads) {
      thread.join();
    }
  });

  int num_threads = 40;

  for (int i = 0; i < num_threads; ++i) {
    std::thread t([&db]() {
      int num_transactions = 10000;
      for (int j = 0; j < num_transactions; ++j) {
        std::this_thread::sleep_for(std::chrono::microseconds(dist(gen) % 50));

        auto txn = db.Begin();
        int64_t key = dist(gen);

        std::this_thread::sleep_for(std::chrono::microseconds(dist(gen) % 50));

        std::string val1 = rand_string();
        txn.Put(key, val1);
        const auto [val, found] = txn.Get(key);
        assert(found);
        assert(val == val1);

        key = dist(gen);
        val1 = rand_string();
        txn.Put(key, val1);

      }
    });
    threads.push_back(std::move(t));
  }

}