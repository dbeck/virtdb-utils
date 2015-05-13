#pragma once

#include "common.pb.h"
#include <string>

// TODO : polymorphic getter

namespace virtdb { namespace interface {

  struct value_type_base
  {
    static bool
    is_null(pb::ValueType & pb_vt,
            int index)
    {
      bool ret = false;
      if( pb_vt.isnull_size() > index )
        ret = pb_vt.isnull(index);
      return ret;
    }

    static void
    set_null(pb::ValueType & pb_vt,
             int index,
             bool val=true)
    {
      // fill the null array up to the
      for( int i=pb_vt.isnull_size(); i<=index; ++i )
        pb_vt.add_isnull(false);
      pb_vt.set_isnull(index, val);
    }
  };
  
  template <typename T> struct value_type {};

  template <>
  struct value_type<std::string> : public value_type_base
  {
    static const pb::Kind kind = pb::Kind::STRING;
    typedef std::string stored_type;

    template <typename ITER>
    static void
    set(pb::ValueType & pb_vt,
        ITER begin,
        ITER end,
        pb::Kind val_kind=kind)
    {
      pb_vt.set_type(val_kind);
      if( pb_vt.stringvalue_size() )
        pb_vt.clear_stringvalue();
      for( auto it=begin ; it != end ; ++it )
        pb_vt.add_stringvalue(*it);
    }
    
    static void
    set(pb::ValueType & pb_vt,
        const stored_type & v,
        pb::Kind val_kind=kind)
    {
      const stored_type * val_ptr = &v;
      set(pb_vt, val_ptr, val_ptr+1, val_kind);
    }

    static int
    size(pb::ValueType & pb_vt)
    {
      return pb_vt.stringvalue_size();
    }
    
    static stored_type
    get(pb::ValueType & pb_vt,
        int index,
        const stored_type & default_value)
    {
      if( pb_vt.stringvalue_size() <= index )
        return default_value;
      else
        return pb_vt.stringvalue(index);
    }
  };
  
  template <>
  struct value_type<const char *> : public value_type_base
  {
    static const pb::Kind kind = pb::Kind::STRING;
    typedef std::string stored_type;
    
    template <typename ITER>
    static void
    set(pb::ValueType & pb_vt,
        ITER begin,
        ITER end,
        pb::Kind val_kind=kind)
    {
      pb_vt.set_type(val_kind);
      if( pb_vt.stringvalue_size() )
        pb_vt.clear_stringvalue();
      for( auto it=begin ; it != end ; ++it )
        pb_vt.add_stringvalue(*it);
    }
    
    static void
    set(pb::ValueType & pb_vt,
        const char * v,
        pb::Kind val_kind=kind)
    {
      const char ** val_ptr = &v;
      set(pb_vt, val_ptr, val_ptr+1, val_kind);
    }

    static int
    size(pb::ValueType & pb_vt)
    {
      return pb_vt.stringvalue_size();
    }
    
    static stored_type
    get(pb::ValueType & pb_vt,
        int index,
        const stored_type & default_value)
    {
      if( pb_vt.stringvalue_size() <= index )
        return default_value;
      else
        return pb_vt.stringvalue(index);
    }
  };
  
  template <>
  struct value_type<int32_t> : public value_type_base
  {
    static const pb::Kind kind = pb::Kind::INT32;
    typedef int32_t stored_type;
    
    template <typename ITER>
    static void
    set(pb::ValueType & pb_vt,
        ITER begin,
        ITER end,
        pb::Kind val_kind=kind)
    {
      pb_vt.set_type(val_kind);
      if( pb_vt.int32value_size() )
        pb_vt.clear_int32value();
      for( auto it=begin ; it != end ; ++it )
        pb_vt.add_int32value(*it);
    }
    
    static void
    set(pb::ValueType & pb_vt,
        stored_type v,
        pb::Kind val_kind=kind)
    {
      const stored_type * val_ptr = &v;
      set(pb_vt, val_ptr, val_ptr+1, val_kind);
    }

    static int
    size(pb::ValueType & pb_vt)
    {
      return pb_vt.int32value_size();
    }
    
    static stored_type
    get(pb::ValueType & pb_vt,
        int index,
        const stored_type & default_value)
    {
      if( pb_vt.int32value_size() <= index )
        return default_value;
      else
        return pb_vt.int32value(index);
    }
  };
  
  template <>
  struct value_type<int64_t> : public value_type_base
  {
    static const pb::Kind kind = pb::Kind::INT64;
    typedef int64_t stored_type;

    template <typename ITER>
    static void
    set(pb::ValueType & pb_vt,
        ITER begin,
        ITER end,
        pb::Kind val_kind=kind)
    {
      pb_vt.set_type(val_kind);
      if( pb_vt.int64value_size() )
        pb_vt.clear_int64value();
      for( auto it=begin ; it != end ; ++it )
        pb_vt.add_int64value(*it);
    }
    
    static void
    set(pb::ValueType & pb_vt,
        stored_type v,
        pb::Kind val_kind=kind)
    {
      const stored_type * val_ptr = &v;
      set(pb_vt, val_ptr, val_ptr+1, val_kind);
    }
    
    static int
    size(pb::ValueType & pb_vt)
    {
      return pb_vt.int64value_size();
    }
    
    static stored_type
    get(pb::ValueType & pb_vt,
        int index,
        const stored_type & default_value)
    {
      if( pb_vt.int64value_size() <= index )
        return default_value;
      else
        return pb_vt.int64value(index);
    }
  };
  
  template <>
  struct value_type<uint32_t> : public value_type_base
  {
    static const pb::Kind kind = pb::Kind::UINT32;
    typedef uint32_t stored_type;

    template <typename ITER>
    static void
    set(pb::ValueType & pb_vt,
        ITER begin,
        ITER end,
        pb::Kind val_kind=kind)
    {
      pb_vt.set_type(val_kind);
      if( pb_vt.uint32value_size() )
        pb_vt.clear_uint32value();
      for( auto it=begin ; it != end ; ++it )
        pb_vt.add_uint32value(*it);
    }
    
    static void
    set(pb::ValueType & pb_vt,
        stored_type v,
        pb::Kind val_kind=kind)
    {
      const stored_type * val_ptr = &v;
      set(pb_vt, val_ptr, val_ptr+1, val_kind);
    }
    
    static int
    size(pb::ValueType & pb_vt)
    {
      return pb_vt.uint32value_size();
    }
    
    static stored_type
    get(pb::ValueType & pb_vt,
        int index,
        const stored_type & default_value)
    {
      if( pb_vt.uint32value_size() <= index )
        return default_value;
      else
        return pb_vt.uint32value(index);
    }
  };
  
  template <>
  struct value_type<uint64_t> : public value_type_base
  {
    static const pb::Kind kind = pb::Kind::UINT64;
    typedef uint64_t stored_type;
    
    template <typename ITER>
    static void
    set(pb::ValueType & pb_vt,
        ITER begin,
        ITER end,
        pb::Kind val_kind=kind)
    {
      pb_vt.set_type(val_kind);
      if( pb_vt.uint64value_size() )
        pb_vt.clear_uint64value();
      for( auto it=begin ; it != end ; ++it )
        pb_vt.add_uint64value(*it);
    }
    
    static void
    set(pb::ValueType & pb_vt,
        stored_type v,
        pb::Kind val_kind=kind)
    {
      const stored_type * val_ptr = &v;
      set(pb_vt, val_ptr, val_ptr+1, val_kind);
    }
    
    static int
    size(pb::ValueType & pb_vt)
    {
      return pb_vt.uint64value_size();
    }
    
    static stored_type
    get(pb::ValueType & pb_vt,
        int index,
        const stored_type & default_value)
    {
      if( pb_vt.uint64value_size() <= index )
        return default_value;
      else
        return pb_vt.uint64value(index);
    }
  };
  
  template <>
  struct value_type<double> : public value_type_base
  {
    static const pb::Kind kind = pb::Kind::DOUBLE;
    typedef double stored_type;

    template <typename ITER>
    static void
    set(pb::ValueType & pb_vt,
        ITER begin,
        ITER end,
        pb::Kind val_kind=kind)
    {
      pb_vt.set_type(val_kind);
      if( pb_vt.doublevalue_size() )
        pb_vt.doublevalue();
      for( auto it=begin ; it != end ; ++it )
        pb_vt.add_doublevalue(*it);
    }
    
    static void
    set(pb::ValueType & pb_vt,
        stored_type v,
        pb::Kind val_kind=kind)
    {
      const stored_type * val_ptr = &v;
      set(pb_vt, val_ptr, val_ptr+1, val_kind);
    }
    
    static int
    size(pb::ValueType & pb_vt)
    {
      return pb_vt.doublevalue_size();
    }
    
    static stored_type
    get(pb::ValueType & pb_vt,
        int index,
        const stored_type & default_value)
    {
      if( pb_vt.doublevalue_size() <= index )
        return default_value;
      else
        return pb_vt.doublevalue(index);
    }
  };
  
  template <>
  struct value_type<float> : public value_type_base
  {
    static const pb::Kind kind = pb::Kind::FLOAT;
    typedef float stored_type;
    
    template <typename ITER>
    static void
    set(pb::ValueType & pb_vt,
        ITER begin,
        ITER end,
        pb::Kind val_kind=kind)
    {
      pb_vt.set_type(val_kind);
      if( pb_vt.floatvalue_size() )
        pb_vt.floatvalue();
      for( auto it=begin ; it != end ; ++it )
        pb_vt.add_floatvalue(*it);
    }
    
    static void
    set(pb::ValueType & pb_vt,
        stored_type v,
        pb::Kind val_kind=kind)
    {
      const stored_type * val_ptr = &v;
      set(pb_vt, val_ptr, val_ptr+1, val_kind);
    }
    
    static int
    size(pb::ValueType & pb_vt)
    {
      return pb_vt.floatvalue_size();
    }
    
    static stored_type
    get(pb::ValueType & pb_vt,
        int index,
        const stored_type & default_value)
    {
      if( pb_vt.floatvalue_size() <= index )
        return default_value;
      else
        return pb_vt.floatvalue(index);
    }
  };
  
  template <>
  struct value_type<bool> : public value_type_base
  {
    static const pb::Kind kind = pb::Kind::BOOL;
    typedef bool stored_type;
    
    template <typename ITER>
    static void
    set(pb::ValueType & pb_vt,
        ITER begin,
        ITER end,
        pb::Kind val_kind=kind)
    {
      pb_vt.set_type(val_kind);
      if( pb_vt.boolvalue_size() )
        pb_vt.boolvalue();
      for( auto it=begin ; it != end ; ++it )
        pb_vt.add_boolvalue(*it);
    }
    
    static void
    set(pb::ValueType & pb_vt,
        bool v,
        pb::Kind val_kind=kind)
    {
      const bool * val_ptr = &v;
      set(pb_vt, val_ptr, val_ptr+1, val_kind);
    }

    static int
    size(pb::ValueType & pb_vt)
    {
      return pb_vt.boolvalue_size();
    }
    
    static stored_type
    get(pb::ValueType & pb_vt,
        int index,
        const stored_type & default_value)
    {
      if( pb_vt.boolvalue_size() <= index )
        return default_value;
      else
        return pb_vt.boolvalue(index);
    }
  };
  
}} // virtdb::interface
