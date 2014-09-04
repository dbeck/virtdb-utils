#include "zmq_utils.hh"
#include "net.hh"
#include "exception.hh"
#include <logger.hh>
#include <sstream>

namespace virtdb { namespace util {
  
  std::pair<std::string, unsigned short>
  parse_zmq_tcp_endpoint(const std::string & ep)
  {
    std::pair<std::string, unsigned short> ret{"",0};
    if( ep.empty() ) { THROW_("empty input"); }
    const char * begin  = ep.c_str();
    const char * end    = begin + ep.size();
    const char * port   = end;
    
    while( port > begin )
    {
      if( *port == ':' )
      {
        if( port > end-2 ) { THROW_("invalid input"); }
        int p = ::atoi(port+1);
        if( p <= 0 || p > 65535 ) { THROW_("cannot parse port"); }
        ret.second = static_cast<unsigned short >(p);
        break;
      }
      --port;
    }
    
    if( ret.second == 0 ) { THROW_("failed to parse port"); }
    
    const char * host = port-1;
    while( host > begin )
    {
      if( host[0] == ':' && host[1] == '/'  && host[2] == '/' )
      {
        ret.first.assign( host+3, port );
        break;
      }
      --host;
    }

    if( ret.first.empty() ) { THROW_("failed to parse host"); }
    
    return ret;
  }
  
  zmq_socket_wrapper::zmq_socket_wrapper(zmq::context_t &ctx, int type)
  : socket_(ctx, type),
    valid_(false),
    valid_future_(valid_promise_.get_future())
  {
  }
  
  zmq_socket_wrapper::~zmq_socket_wrapper()
  {
  }
  
  zmq::socket_t &
  zmq_socket_wrapper::get()
  {
    return socket_;
  }
  
  void
  zmq_socket_wrapper::batch_tcp_bind(const host_set & hosts)
  {
    for( auto const & host : hosts )
    {
      std::ostringstream os;
      if( host.empty() ) continue;

      if( host.find(":") != std::string::npos && host[0] != '[')
        os << "tcp://[" << host << "]:*";
      else
        os << "tcp://" << host << ":*";
      
      try
      {
        bind(os.str().c_str());
      }
      catch (const std::exception & e)
      {
        // ignore errors
        std::string text{e.what()};
        std::string endpoint{os.str()};
        LOG_ERROR("exception caught" << V_(text) << "while trying to bind to" << V_(endpoint));
      }
    }
  }
  
  void
  zmq_socket_wrapper::bind (const char *addr)
  {
    if( !addr ) return;
    
    socket_.bind(addr);
    set_valid();
    
    if( strncmp(addr,"tcp://",6) == 0 )
    {
      int last_zmq_port = 0;
      char last_zmq_endpoint[1024];
      size_t opt_size = sizeof(last_zmq_endpoint);
      
      // zero terminated if getsockopt failes
      last_zmq_endpoint[0] = 0;
      socket_.getsockopt(ZMQ_LAST_ENDPOINT, last_zmq_endpoint, &opt_size);
      
      // make sure we have a zero terminated string
      last_zmq_endpoint[sizeof(last_zmq_endpoint)-1] = 0;
      
      if( opt_size > 0 )
      {
        char * ptr1 = last_zmq_endpoint+opt_size;
        while( ptr1 > last_zmq_endpoint )
        {
          if( *ptr1 == ':' )
          {
            last_zmq_port = atoi(ptr1+1);
            break;
          }
          --ptr1;
        }
        
        if( *ptr1 == ':' )
        {
          char * ptr0 = last_zmq_endpoint+6;
          
          if( ptr0 < ptr1 && last_zmq_port > 0 )
          {
            std::string last_zmq_host(ptr0,ptr1);
            net::string_vector own_ips;
            if( last_zmq_host != "0.0.0.0" )
            {
              own_ips.push_back(last_zmq_host);
            }
            else
            {
              own_ips = net::get_own_ips(true);
            }
            for( auto const & ip : own_ips )
            {
              std::ostringstream os;
              if( ip.find(":") != std::string::npos )
              {
                os << "tcp://[" << ip << "]:" << last_zmq_port;
              }
              else
              {
                os << "tcp://" << ip << ':' << last_zmq_port;
              }
              endpoints_.insert(os.str());
            }
          }
        }
      }
    }
    else
    {
      endpoints_.insert(addr);
    }
  }
  
  void
  zmq_socket_wrapper::connect (const char * addr)
  {
    if( !addr ) return;
    socket_.connect(addr);
    endpoints_.insert(addr);
    set_valid();
  }
  
  void
  zmq_socket_wrapper::reconnect (const char * addr)
  {
    if( !addr ) return;
    if( endpoints_.count(addr) > 0 ) return;
    disconnect_all();
    connect(addr);
    set_valid();
  }
  
  void
  zmq_socket_wrapper::disconnect_all()
  {
    // disconnect from all endpoints if any
    for( auto & ep : endpoints_ )
      socket_.disconnect(ep.c_str());
    endpoints_.clear();
    set_invalid();
  }
  
  void
  zmq_socket_wrapper::set_valid()
  {
    if( !valid_ )
      valid_promise_.set_value();
    valid_ = true;
  }
  
  void
  zmq_socket_wrapper::set_invalid()
  {
    // reset future and promise
    {
      std::promise<void> new_promise;
      valid_promise_.swap(new_promise);
      valid_future_ = valid_promise_.get_future();
    }
    valid_ = false;
  }
  
  bool
  zmq_socket_wrapper::valid() const
  {
    return valid_;
  }
  
  bool
  zmq_socket_wrapper::wait_valid(unsigned long ms)
  {
    if( valid_ )
      return true;
    
    std::future_status status = valid_future_.wait_for(std::chrono::milliseconds(ms));
    if( status == std::future_status::ready )
      return true;
    else
      return false;
  }

  void
  zmq_socket_wrapper::wait_valid()
  {
    if( valid_ ) return;
    valid_future_.wait();
    return;
  }
  
  const zmq_socket_wrapper::endpoint_set &
  zmq_socket_wrapper::endpoints() const
  {
    return endpoints_;
  }
  
  bool
  zmq_socket_wrapper::poll_in(unsigned long ms)
  {
    if( !valid_ )
      return false;
    
    // interested in incoming messages
    zmq::pollitem_t poll_item{
      socket_,
      0,
      ZMQ_POLLIN,
      0
    };
    
    // willing to wait for 3s for new messages
    if( zmq::poll(&poll_item, 1, ms) == -1 ||
       !(poll_item.revents & ZMQ_POLLIN) )
    {
      return false;
    }
    else
    {
      return true;
    }
  }

}}
