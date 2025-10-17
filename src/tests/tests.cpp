#include <gtest/gtest.h>
#include <memory>
#include "kvstore/kv-store.hpp"

TEST(basic, basic1) {
  DB<int, std::string> db;
  using Txn = DB<int, std::string>::Txn;
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
  DB<int, std::string> db;
  using Txn = DB<int, std::string>::Txn;
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
  DB<int, std::string> db;
  using Txn = DB<int, std::string>::Txn;
  Txn t1 = db.Begin();
  Txn t2 = db.Begin();
  t1.Put(3, "asdf");
  t2.Get(4);
  t1.Commit();
  t2.Commit();
}

// A test with a set of bank accounts.
// Each txn transfers money from 1 to another.
// Read only txns that get sum of numbers.
// Assert they get the full amount.
class InvariantTests : public testing::Test {
protected:
  InvariantTests() {
    db_.reset(new DB<int, int>{});
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
    auto txn = db_->Begin();
    long total_money = 0;
    for (int acc : accounts_) {
      auto [acc_money, found] = txn.Get(acc);
      EXPECT_TRUE(found);
      total_money += acc_money;
    }
    txn.Commit();
    return total_money;
  }

  std::unique_ptr<DB<int, int>> db_;
  std::vector<int> accounts_;
  int num_accounts_ = 10;
  int total_money_;
};

TEST_F(InvariantTests, CheckTotalMoney) {
  long total_money = GetTotalMoney();
  EXPECT_EQ(total_money, total_money_);
}

