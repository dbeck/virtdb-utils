#pragma once
#include <cstdint>
#include <string>

namespace virtdb { namespace utils {
  
  struct utf8
  {
    static void sanitize(char * p, size_t len);
  };
  
}}
