#pragma once

#include <queue>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <chrono>
#include <functional>
#include <vector>
#include <atomic>
#include <algorithm>
#include "barrier.hh"
#include "exception.hh"
#include <logger.hh>

namespace virtdb { namespace util {

  template <typename ITEM, unsigned int WAKEUP_FREQ=500>
  class active_queue final
  {
    typedef std::mutex                   mtx;
    typedef std::queue<ITEM>             q;
    typedef std::condition_variable      cond;
    typedef std::vector<std::thread>     thread_vector;
    typedef std::atomic<bool>            flag;
    typedef std::unique_lock<mtx>        lock;
    
    active_queue() = delete;
    active_queue(const active_queue&) = delete;
    active_queue & operator=(const active_queue &) = delete;
    
  public:
    typedef std::function<void(ITEM)> item_handler;
    
    static const unsigned int wakeup_freq() { return WAKEUP_FREQ; }
    
    active_queue(unsigned int nthreads, item_handler handler)
    : barrier_(nthreads+1),
      handler_(handler),
      stop_(false)
    {
      // starting all threads in the constructor
      for( unsigned int i=0; i<nthreads; ++i )
      {
        threads_.push_back(std::move(std::thread(std::bind(&active_queue::entry,this))));
      }
      
      // this won't return till all threads are ready
      barrier_.wait();
      
      // give a chance to the workers to reach wait() before this
      // thread start sending in the items
      std::this_thread::yield();
    }
    
    void push(const ITEM & i)
    {
      if( stop_ ) return;
      lock l(mutex_);
      queue_.push(i);
      cond_.notify_one();
    }
    
    void push(ITEM && i)
    {
      if( stop_ ) return;
      lock l(mutex_);
      queue_.push(std::move(i));
      cond_.notify_one();
    }
    
    ~active_queue()
    {
      stop_ = true;
      cond_.notify_all();
      std::for_each(threads_.begin(),
                    threads_.end(),
                    std::mem_fn(&std::thread::join));
    }
    
  private:
    mtx             mutex_;
    cond            cond_;
    q               queue_;
    barrier         barrier_;
    item_handler    handler_;
    thread_vector   threads_;
    flag            stop_;
    
    void entry()
    {
      // synchronize between the threads and the constructor
      barrier_.wait();
      
      // check if we can still run
      while( stop_ == false )
      {
        ITEM tmp;
        bool has_item = false;
        {
          lock l(mutex_);
          
          cond_.wait_for(l,
                         std::chrono::milliseconds(WAKEUP_FREQ),
                         [this] { return !queue_.empty(); }
                         );
          
          if( !queue_.empty() )
          {
            // we first dequeue the element, so we don't need to hold
            // the lock while the thread handler runs
            has_item = true;
            tmp = std::move(queue_.front());
            queue_.pop();
          }
          
        }
        
        // the thread now processes the item outside the lock
        if( has_item )
        {
          try
          {
            handler_(std::move(tmp));
          }
          catch( const std::exception & e )
          {
            std::string exception_text(e.what());
            LOG_ERROR("exception caught: " << exception_text);
          }
          catch(...)
          {
            LOG_ERROR("unknown exception caught");
          }
        }
      }
    }
  };

}}
