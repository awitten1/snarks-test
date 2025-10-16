#include <gtest/gtest.h>
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
  }
  {
    Txn t = db.Begin();
    auto x = t.Get(3);
    EXPECT_TRUE(x.second);
    EXPECT_EQ(x.first, "asdf");
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