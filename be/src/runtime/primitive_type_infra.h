// This file is made available under Elastic License 2.0.

#pragma once

#include "common/logging.h"
#include "runtime/primitive_type.h"

namespace starrocks {
    
// type_dispatch_all: 
// macro to enumate all types
#define TYPE_DISPATCH_ALL(TEMPLATE_FUNC, typeKind, ...)             \
[&]() {                                                         \
    switch (typeKind) {                                         \
    case TYPE_INT: return TEMPLATE_FUNC<TYPE_INT>(__VA_ARGS__); \
    default: CHECK(false) << "TODO";                            \
    }                                                           \
}();


// type_dispatch_all: 
// Dispatch dynamic ptype to static template instance Functor
template <class Functor>
auto type_dispatch_all(PrimitiveType ptype, Functor fun) {
    
#define _TYPE_DISPATCH_CASE(type) \
    case type: return fun.template operator()<type>(); 
    
    switch (ptype) {
    _TYPE_DISPATCH_CASE(TYPE_NULL)
    _TYPE_DISPATCH_CASE(TYPE_BOOLEAN)          
    _TYPE_DISPATCH_CASE(TYPE_TINYINT)          
    _TYPE_DISPATCH_CASE(TYPE_SMALLINT)         
    _TYPE_DISPATCH_CASE(TYPE_INT)              
    _TYPE_DISPATCH_CASE(TYPE_BIGINT)           
    _TYPE_DISPATCH_CASE(TYPE_LARGEINT)         
    _TYPE_DISPATCH_CASE(TYPE_FLOAT)            
    _TYPE_DISPATCH_CASE(TYPE_DOUBLE)           
    _TYPE_DISPATCH_CASE(TYPE_VARCHAR)          
    _TYPE_DISPATCH_CASE(TYPE_DATE)             
    _TYPE_DISPATCH_CASE(TYPE_DATETIME)         
    // _TYPE_DISPATCH_CASE(TYPE_BINARY)           
    // _TYPE_DISPATCH_CASE(TYPE_DECIMAL)          
    _TYPE_DISPATCH_CASE(TYPE_CHAR)             
    // _TYPE_DISPATCH_CASE(TYPE_STRUCT)           
    // _TYPE_DISPATCH_CASE(TYPE_ARRAY)            
    // _TYPE_DISPATCH_CASE(TYPE_MAP)              
    _TYPE_DISPATCH_CASE(TYPE_HLL)              
    _TYPE_DISPATCH_CASE(TYPE_TIME)             
    _TYPE_DISPATCH_CASE(TYPE_OBJECT)           
    _TYPE_DISPATCH_CASE(TYPE_PERCENTILE)       
    _TYPE_DISPATCH_CASE(TYPE_DECIMALV2)        
    _TYPE_DISPATCH_CASE(TYPE_DECIMAL32)        
    _TYPE_DISPATCH_CASE(TYPE_DECIMAL64)        
    _TYPE_DISPATCH_CASE(TYPE_DECIMAL128)       
    default:
        LOG(FATAL) << "Unknown type " << ptype;
    }

#undef _TYPE_DISPATCH_CASE
}


} // namespace starrocks
