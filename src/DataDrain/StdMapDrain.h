//
//  StdMapDrain.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//
#pragma once

#include <mutex>
#include <unordered_map>

namespace Thilenius {
namespace MapReduce {
namespace DataDrain {

template<typename ReducePolicy> class StdMapDrain {
 public:
  typedef std::unordered_map<typename ReducePolicy::KeyType,
          typename ReducePolicy::ValueType > InputType;

  StdMapDrain() :
    m_data(new InputType()) {

  }

  StdMapDrain(InputType* destinationMap) :
    m_data(destinationMap) {

  }

  // Thread Safe
  inline void AddData(typename ReducePolicy::KeyType key,
                      typename ReducePolicy::ValueType value) {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_data->insert( { key, value } );
  }

 private:
  InputType* m_data;
  std::mutex m_mutex;
};

} // namespace DataDrain
} // namespace MapReduce
} // namespace Thilenius
