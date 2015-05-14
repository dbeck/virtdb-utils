#include <gtest/gtest.h>
#include <utils/active_queue.hh>
#include <utils/async_worker.hh>
#include <utils/net.hh>
#include <utils/exception.hh>
#include <utils/utf8.hh>
#include <utils/table_collector.hh>
#include <utils/relative_time.hh>
#include <future>
#include <thread>
#include <atomic>
#include <memory>

using namespace virtdb::utils;

// macro helpers for concatenating variable names
#define INTERNAL_MACRO_CONCAT(A,B) __INTERNAL__##A##B
#define INTERNAL_MACRO_CONCAT2(A,B) INTERNAL_MACRO_CONCAT(A,B)
#define INTERNAL_LOCAL_VAR(A) INTERNAL_MACRO_CONCAT2(A,__LINE__)

namespace
{
  struct measure
  {
    std::string    file_;
    int            line_;
    std::string    function_;
    relative_time  rt_;
    
    measure(const char * f,
            int l,
            const char * fn)
    : file_(f),
    line_(l),
    function_(fn) {}
    
    ~measure()
    {
      double tm = rt_.get_usec() / 1000.0;
      std::cout << "[" << file_ << ':' << line_ << "] " << function_ << "() " << tm << " ms\n";
    }
  };
}

#define MEASURE_ME measure INTERNAL_LOCAL_VAR(_m_) { __FILE__, __LINE__, __func__ };

namespace virtdb { namespace test {
  
  class UtilActiveQueueTest : public ::testing::Test
  {
  protected:
    UtilActiveQueueTest();
    
    std::atomic<int> value_;
    active_queue<int,100> queue_;
  };
  
  class UtilBarrierTest : public ::testing::Test
  {
  protected:
    UtilBarrierTest();
    barrier barrier_;
  };
  
  class UtilNetTest : public ::testing::Test { };
  class UtilFlexAllocTest : public ::testing::Test { };
  class UtilAsyncWorkerTest : public ::testing::Test { };
  class UtilTableCollectorTest : public ::testing::Test { };
  class UtilUtf8Test : public ::testing::Test { };
}}

using namespace virtdb::test;

TEST_F(UtilTableCollectorTest, MultiThreaded)
{
  table_collector<int> q{3};
  auto entry = [&q] {
    for( size_t i = 0; i < 10000; ++i )
    {
      for( size_t ii = 0; ii<3; ++ii )
      {
        q.insert(i, ii, std::shared_ptr<int>(new int {(int)((10000*ii)+i)}));
      }
    }
  };
  {
    std::thread publisher(entry);
    MEASURE_ME;
    {
      MEASURE_ME;
      for( size_t i=0; i<10000; ++i )
      {
        auto ro = q.get(i,30000);
      }
    }
    publisher.join();
  }
}

TEST_F(UtilTableCollectorTest, MultiThreaded2)
{
  table_collector<int> q{3};
  auto entry = [&q] {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    for( size_t i = 0; i < 10000; ++i )
    {
      for( size_t ii = 0; ii<3; ++ii )
      {
        q.insert(i, ii, std::shared_ptr<int>(new int {(int)((10000*ii)+i)}));
      }
    }
  };
  {
    std::thread publisher(entry);
    MEASURE_ME;
    {
      MEASURE_ME;
      for( size_t i=0; i<10000; ++i )
      {
        auto ro = q.get(i,30000);
      }
    }
    publisher.join();
  }
}

TEST_F(UtilTableCollectorTest, Basic)
{
  table_collector<int> q(3);
  EXPECT_FALSE(q.stopped());
  auto const & val0 = q.get(0,200);
  EXPECT_EQ(val0.first.size(), 3);
  EXPECT_EQ(val0.second, 0);
  std::shared_ptr<int> i(new int{1});
  q.insert(0, 0, i);
  q.insert(0, 1, i);
  auto const & val1 = q.get(0,200);
  EXPECT_FALSE(val1.first.empty());
  EXPECT_NE(val1.second, 3);
  q.insert(0, 2, i);
  auto const & val2 = q.get(0,200);
  EXPECT_FALSE(val2.first.empty());
  EXPECT_EQ(val2.second, 3);
  EXPECT_EQ(val2.first.size(), 3);
  // test timeout too
  relative_time rt;
  auto const & val3 = q.get(0,2000000);
  EXPECT_FALSE(val3.first.empty());
  EXPECT_EQ(val3.second, 3);
  // I expect to have results immediately. give a bit
  // of time for table_collector anyway
  EXPECT_LT(rt.get_msec(), 10);
}

TEST_F(UtilTableCollectorTest, Timeout)
{
  //MEASURE_ME;
  table_collector<int> q(2);
  q.insert(0, 0, new int{0});
  q.insert(0, 1, new int{1});
  q.insert(2, 0, new int{2});
  q.insert(2, 1, new int{3});
  q.insert(4, 0, new int{4});
  q.insert(4, 1, new int{5});
  
  // get(0)
  size_t thousand_ms = 1000;
  {
    auto row = q.get(0,thousand_ms);
    EXPECT_EQ(row.first.size(), 2);
    EXPECT_EQ(row.second, 2);
  }
  
  // get(1)
  {
    auto row = q.get(1,thousand_ms);
    EXPECT_EQ(row.first.size(), 2);
    EXPECT_EQ(row.second, 0);
  }
  
  // get(2)
  {
    auto row = q.get(2,thousand_ms);
    EXPECT_EQ(row.first.size(), 2);
    EXPECT_EQ(row.second, 2);
  }
}


TEST_F(UtilTableCollectorTest, AllOps)
{
  table_collector<int> q(2);
  q.insert(0, 0, new int{0});
  q.insert(0, 1, new int{1});
  q.insert(1, 0, new int{2});
  q.insert(1, 1, new int{3});
  q.insert(2, 1, new int{4});
  
  // double set 0,0
  q.insert(0, 0, new int{9});
  
  // missing columns
  EXPECT_EQ(q.missing_columns(0), 0);
  EXPECT_EQ(q.missing_columns(1), 0);
  EXPECT_EQ(q.missing_columns(2), 1);
  
  // erase + missing
  q.erase(1);
  EXPECT_EQ(q.missing_columns(0), 0);
  EXPECT_EQ(q.missing_columns(1), 2);
  EXPECT_EQ(q.missing_columns(2), 1);
  
  // get(0)
  size_t one_ms = 1;
  {
    auto row = q.get(0,one_ms);
    EXPECT_EQ(row.first.size(), 2);
    EXPECT_EQ(row.second, 2);
    EXPECT_EQ(*row.first[0], 9);
    EXPECT_EQ(*row.first[1], 1);
  }
  
  // get(1)
  {
    auto row = q.get(1,one_ms);
    EXPECT_EQ(row.first.size(), 2);
    EXPECT_EQ(row.second, 0);
  }
  
  // get(2)
  {
    auto row = q.get(2,one_ms);
    EXPECT_EQ(row.first.size(), 2);
    EXPECT_EQ(row.second, 1);
    EXPECT_EQ(*row.first[1], 4);
  }
}

UtilActiveQueueTest::UtilActiveQueueTest()
: value_(0),
queue_(10,[this](int v){ value_ += v; })
{
}

TEST_F(UtilActiveQueueTest, LongSleep)
{
  {
    active_queue<int,100> q{
      1,
      [](int i)
      {
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
      }};
    EXPECT_TRUE( q.wait_empty(std::chrono::milliseconds(1)) );
    EXPECT_TRUE( q.wait_empty(std::chrono::milliseconds(1000)) );
    q.push(1);
    {
      MEASURE_ME;
      EXPECT_FALSE( q.wait_empty(std::chrono::milliseconds(1)) );
    }
    {
      MEASURE_ME;
      EXPECT_TRUE( q.wait_empty(std::chrono::milliseconds(6000)) );
    }
  }
  {
    active_queue<int,3000> q{
      1,
      [](int i)
      {
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
      }};
    EXPECT_TRUE( q.wait_empty(std::chrono::milliseconds(1)) );
    EXPECT_TRUE( q.wait_empty(std::chrono::milliseconds(1000)) );
    q.push(1);
    {
      MEASURE_ME;
      EXPECT_FALSE( q.wait_empty(std::chrono::milliseconds(1)) );
    }
    {
      MEASURE_ME;
      EXPECT_TRUE( q.wait_empty(std::chrono::milliseconds(6000)) );
    }
  }
}

TEST_F(UtilActiveQueueTest, AddNumbers)
{
  for( int i=1; i<=10000; ++i )
    this->queue_.push(i);
  
  {
    relative_time rt;
    EXPECT_TRUE( this->queue_.wait_empty(std::chrono::milliseconds(20000)) );
    std::cout << "took " << rt.get_usec() << " usec\n";
  }
  this->queue_.stop();
  EXPECT_TRUE( this->queue_.stopped() );
  EXPECT_EQ( this->value_, 50005000 );
}

TEST_F(UtilActiveQueueTest, Stop3Times)
{
  this->queue_.stop();
  EXPECT_TRUE( this->queue_.stopped() );
  this->queue_.stop();
  EXPECT_TRUE( this->queue_.stopped() );
  this->queue_.stop();
  EXPECT_TRUE( this->queue_.stopped() );
}

UtilBarrierTest::UtilBarrierTest() : barrier_(10) {}

TEST_F(UtilBarrierTest, BarrierReady)
{
  std::atomic<int> flag{0};
  std::vector<std::thread> threads;
  
  // start 9 threads an make sure they are all waiting
  for( int i=0; i<9; ++i )
  {
    EXPECT_FALSE( this->barrier_.ready() );
    auto & b = this->barrier_;
    threads.push_back(std::thread{[&b,&flag](){
      if( b.wait_for(100) == false )
      {
        b.wait();
      }
      ++flag;
    }});
    // all threads are waiting
    EXPECT_EQ(flag, 0);
    EXPECT_FALSE(b.ready());
  }
  
  // let the detached threads go by doing a wait on the barrier
  this->barrier_.wait();
  EXPECT_TRUE( this->barrier_.ready() );
  
  // cleanup threads
  for( auto & t : threads )
  {
    if( t.joinable() )
      t.join();
  }
  
  EXPECT_EQ(flag, 9);
}

TEST_F(UtilNetTest, DummyTest)
{
  // TODO : NetTest
}

TEST_F(UtilFlexAllocTest, DummyTest)
{
  // TODO : FlexAllocTest
}

TEST_F(UtilAsyncWorkerTest, DestroyWithoutStart)
{
  auto fun = [](void) {
    throw std::logic_error("hello");
    return true;
  };
  
  {
    async_worker worker{fun,0,false};
  }
}


TEST_F(UtilAsyncWorkerTest, CatchThreadException)
{
  auto fun = [](void) {
    throw std::logic_error("hello");
    return true;
  };
  async_worker worker{fun,0,false};
  worker.start();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  
  EXPECT_THROW(worker.rethrow_error(), std::logic_error);
}

TEST_F(UtilUtf8Test, Valid)
{
  char simple[] = u8"árvíztűrő tükörfúrógép. ÁRVÍZTŰRŐ TÜKÖRFÚRÓGÉP";
  std::string simple_str{u8"árvíztűrő tükörfúrógép. ÁRVÍZTŰRŐ TÜKÖRFÚRÓGÉP"};
  EXPECT_GT(sizeof(simple), 10);
  
  utf8::sanitize(simple, simple_str.size());
  EXPECT_EQ(simple_str, simple);
}

TEST_F(UtilUtf8Test, InValid)
{
  char simple[] = "árvíztűrő tükörfúrógép. ÁRVÍZTŰRŐ TÜKÖRFÚRÓGÉP";
  std::string simple_str{u8"árvíztűrő tükörfúrógép. ÁRVÍZTŰRŐ TÜKÖRFÚRÓGÉP"};
  
  utf8::sanitize(simple, simple_str.size());
  EXPECT_EQ(simple_str, simple);
}

TEST_F(UtilUtf8Test, ZeroChar)
{
  char simple[] = "X\0X";
  std::string simple_str{"X X"};
  
  utf8::sanitize(simple, simple_str.size());
  EXPECT_EQ(simple_str, simple);
}

TEST_F(UtilUtf8Test, BadContinuation)
{
  char c = 128;
  char str[] = { 'X', c ,'X', 0 };
  std::string str2{"X X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Short2ByteSeq)
{
  char c = 128+64;
  char str[] = { 'X', c ,'X', 0 };
  std::string str2{"X X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Long2ByteSeq)
{
  char tb = 128+64;
  char c = 128+1;
  char str[] = { 'X', tb, c, c ,'X', 0 };
  std::string str2{"X   X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Short3ByteSeq1)
{
  char tb = 128+64+32;
  char str[] = { 'X', tb, 'X', 0 };
  std::string str2{"X X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Short3ByteSeq2)
{
  char tb = 128+64+32;
  char c  = 128+4;
  char str[] = { 'X', tb, c ,'X', 0 };
  std::string str2{"X  X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Long3ByteSeq)
{
  char tb = 128+64+32;
  char c = 128+1;
  char str[] = { 'X', tb, c, c, c, c,'X', 0 };
  std::string str2{"X     X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Short4ByteSeq1)
{
  char tb = 128+64+32+16;
  char str[] = { 'X', tb, 'X', 0 };
  std::string str2{"X X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Short4ByteSeq2)
{
  char tb = 128+64+32+16;
  char c  = 128+4;
  char str[] = { 'X', tb, c ,'X', 0 };
  std::string str2{"X  X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Short4ByteSeq3)
{
  char tb = 128+64+32+16;
  char c  = 128+4;
  char str[] = { 'X', tb, c, c,'X', 0 };
  std::string str2{"X   X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Long4ByteSeq)
{
  char tb = 128+64+32+16;
  char c = 128+1;
  char str[] = { 'X', tb, c, c, c, c, c,'X', 0 };
  std::string str2{"X      X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Mixed1)
{
  char tb1 = 128;
  char tb2 = 128+64;
  char tb3 = 128+64+32;
  char tb4 = 128+64+32+16;
  char c = 128+1;
  char str[] = { 'X', tb1, tb2, tb3, tb4, c, 'X', 0 };
  std::string str2{"X     X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Mixed2)
{
  char tb1 = 128;
  char tb2 = 128+64;
  char tb3 = 128+64+32;
  char tb4 = 128+64+32+16;
  char c = 128+1;
  char str[] = { 'X', tb4, tb3, tb2, tb1, c, 'X', 0 };
  std::string str2{"X     X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

TEST_F(UtilUtf8Test, Garbage)
{
  char c = 0xff;
  char str[] = { 'X', c, 'X', 0 };
  std::string str2{"X X"};
  
  utf8::sanitize(str, str2.size());
  EXPECT_EQ(str2, str);
}

int main(int argc, char ** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  auto ret = RUN_ALL_TESTS();
  return ret;
  return 0;
}
