#include "kvstore/kv-store.hpp"
#include <cstdint>
#include <cstdlib>
#include <thread>

#include <random>
#include <string>


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
  for (int i = 0; i < 100; ++i) {
    std::thread t([&db]() {
      int num_transactions = 100;
      for (int i = 0; num_transactions; ++i) {
        auto txn = db.Begin();
        int64_t key = dist10000(gen) % 1000;
        txn.Put(key, rand_string());
      }
    });
  }

}