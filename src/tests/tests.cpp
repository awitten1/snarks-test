#include <chrono>
#include <condition_variable>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include "kvstore/kv-store.hpp"
#include <cstdint>

TEST(basic, basic1) {
  DB::DB<int, std::string> db;
  using Txn = DB::DB<int, std::string>::Txn;
  {
    Txn t = db.Begin();
    t.Put(3, "asdf");
    auto x = t.Get(3);
    EXPECT_TRUE(x.second);
    EXPECT_EQ(x.first, "asdf");
    t.Commit();
  }
  {
    Txn t = db.Begin();
    auto x = t.Get(3);
    EXPECT_TRUE(x.second);
    EXPECT_EQ(x.first, "asdf");
    t.Commit();
  }
}

TEST(basic, conflict) {
  DB::DB<int, std::string> db;
  using Txn = DB::DB<int, std::string>::Txn;
  Txn t1 = db.Begin();
  Txn t2 = db.Begin();
  t1.Put(3, "asdf");
  t2.Get(3);
  t1.Commit();

  bool got_exception = false;
  try {
    t2.Commit();
  } catch(const TxnConflict&) {
    got_exception = true;
  }
  EXPECT_TRUE(got_exception);
}

TEST(basic, noconflict) {
  DB::DB<int, std::string> db;
  using Txn = DB::DB<int, std::string>::Txn;
  Txn t1 = db.Begin();
  Txn t2 = db.Begin();
  t1.Put(3, "asdf");
  t2.Get(4);
  t1.Commit();
  t2.Commit();
}

TEST(basic, conflict2) {
  DB::DB<int, std::string> db;
  using Txn = DB::DB<int, std::string>::Txn;
  Txn t1 = db.Begin();
  Txn t2 = db.Begin();
  t1.Get(3);
  t1.Get(4);
  t1.Put(3, "asdf");
  t1.Put(4, "asdf1");

  t2.Get(3);
  t2.Get(5);
  t2.Put(3, "asdf");
  t2.Put(5, "asdf1");
  t1.Commit();

  bool got_exception = false;
  try {
    t2.Commit();
  } catch (const TxnConflict& e) {
    got_exception = true;
  }
  ASSERT_TRUE(got_exception);
}

// A test with a set of bank accounts.
// Each txn transfers money from 1 to another.
// Read only txns that get sum of numbers.
// Assert they get the full amount.
class InvariantTests : public testing::Test {
protected:
  InvariantTests() {
    db_.reset(new DB::DB<int64_t, int64_t>{});
    auto txn = db_->Begin();
    long total_money = 0;
    for (int i = 0; i < num_accounts_; ++i) {
      long account_money = 1 + (rand() % 100);
      total_money += account_money;
      txn.Put(i, account_money);
      accounts_.push_back(i);
    }
    txn.Commit();
    total_money_ = total_money;
  }

  long GetTotalMoney() {
    long total_money = 0;

    DB::RetryLoop(*db_, [&](auto& txn) {
      long ltotal_money = 0;
      for (int acc : accounts_) {
        auto [acc_money, found] = txn.Get(acc);
        EXPECT_TRUE(found);
        ltotal_money += acc_money;
      }
      total_money = ltotal_money;
    }, 1000);

    return total_money;
  }

  std::unique_ptr<DB::DB<int64_t, int64_t>> db_;
  std::vector<int> accounts_;
  int num_accounts_ = 10;

  // const after initialization.
  int total_money_;
};

TEST_F(InvariantTests, CheckTotalMoney) {
  long total_money = GetTotalMoney();
  EXPECT_EQ(total_money, total_money_);
}

TEST_F(InvariantTests, CheckTotalMoneyMultipleThreads) {
  bool shutdown = false;
  std::mutex m;
  std::condition_variable cv;

  long total_money = GetTotalMoney();
  EXPECT_EQ(total_money, total_money_);

  //db_->DumpValues();

  std::thread t([&, this]() {
    for (;;) {
      ASSERT_EQ(GetTotalMoney(), total_money_);
      {
        std::unique_lock<std::mutex> lg(m);
        cv.wait_for(lg, std::chrono::milliseconds(1), [&]() {
          return shutdown;
        });
        if (shutdown) {
          return;
        }
      }
    }
  });

  auto th = [&]() {
    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<int64_t> uniform_dist(0);

    auto get_acc = [&]() -> std::pair<int, int> {
      int acc1 = accounts_[uniform_dist(e1) % num_accounts_];
      int acc2 = acc1;
      while (acc2 == acc1)
        acc2 = accounts_[uniform_dist(e1) % num_accounts_];
      return {acc1, acc2};
    };

    for (int i = 0; i < 1000; ++i) {
      // long ltotal_money = GetTotalMoney();
      // ASSERT_EQ(ltotal_money, total_money_);
      DB::RetryLoop(*db_, [&](auto& txn) {
        auto [acc1, acc2] = get_acc();

        auto [amt1, found1] = txn.Get(acc1);
        auto [amt2, found2] = txn.Get(acc2);

        EXPECT_TRUE(found1);
        EXPECT_TRUE(found2);

        int64_t amt_to_deduct = uniform_dist(e1) % amt1;

        //std::cout << acc1 << " -" << amt_to_deduct << "-> " << acc2 << std::endl;
        txn.Put(acc1, amt1 - amt_to_deduct);
        txn.Put(acc2, amt2 + amt_to_deduct);


      }, 1000, 1.2);
      //db_->DumpValues();

    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 10; ++i) {
    threads.push_back(std::thread(th));
  }

  for (auto& t : threads) {
    t.join();
  }

  total_money = GetTotalMoney();
  EXPECT_EQ(total_money, total_money_);

  {
    std::unique_lock<std::mutex> lg(m);
    shutdown = true;
    cv.notify_all();
  }
  t.join();
}

