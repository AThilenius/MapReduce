//
//  StdMapBuffer.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//
#pragma once

#include <atomic>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace Thilenius {
namespace MapReduce {
namespace DataBuffer {

template<typename MapPolicy> class StdMapVectorBuffer {
 public:
  typedef std::vector<typename MapPolicy::IntermediateValueType>
      ValueVectorType;

  typedef std::unordered_map<typename MapPolicy::IntermediateKeyType,
          ValueVectorType*>
              InputType;

  typedef typename std::unordered_map<
      typename MapPolicy::IntermediateKeyType,
               ValueVectorType*>::iterator
                   InputTypeIterator;

  typedef typename std::unordered_map<
      typename MapPolicy::IntermediateKeyType,
               ValueVectorType*>::const_iterator
                   InputTypeConstIterator;

  typedef typename ValueVectorType::iterator
      ValueVectorIteratorType;

  template<typename KeyType, typename ValueType>
      struct Tupple {
        Tupple(KeyType key, ValueType value) :
          Key(key),
          Value(value) {
          }
        KeyType Key;
        ValueType Value;
      };

  typedef Tupple<typename MapPolicy::IntermediateKeyType,
          std::vector<typename MapPolicy::IntermediateValueType>*>
              TupleType;

  typedef typename std::vector<TupleType>
      VectorOfTupplesType;

  StdMapVectorBuffer():
    m_data(new InputType()),
    m_drainData(nullptr) {
    }

  ~StdMapVectorBuffer() {
    SAFE_FREE(m_data);
  }

  // Mutex, Thread Safe
  inline void Combine(StdMapVectorBuffer<MapPolicy>* other) {
    std::lock_guard<std::mutex> guard(m_mutex);

    // For each key in other's data
    InputTypeIterator Otheriter = other->m_data->begin();
    for (; Otheriter != other->m_data->end(); Otheriter++) {
      // Find that key in our data, or create it.
      // Source Vector: Otheriter->second
      // Target Vector: kvpLookup->second or created vector
      InputTypeConstIterator kvpLookup = m_data->find(Otheriter->first);

      if (kvpLookup == m_data->end()) {
        ValueVectorType* vector = new ValueVectorType();
        vector->template insert<ValueVectorIteratorType>(vector->end(),
           Otheriter->second->begin(), Otheriter->second->end());
        m_data->insert( { Otheriter->first, vector } );
      }
      else
        kvpLookup->second->template insert<ValueVectorIteratorType>(
            kvpLookup->second->end(), Otheriter->second->begin(),
            Otheriter->second->end());
    }
  }

  inline void AddData(typename MapPolicy::IntermediateKeyType key,
                      typename MapPolicy::IntermediateValueType value) {
    InputTypeConstIterator kvpLookup = m_data->find(key);

    if (kvpLookup == m_data->end()) {
      ValueVectorType* vector = new ValueVectorType();
      vector->reserve(100);
      vector->push_back(value);
      m_data->insert(std::pair<
                     typename MapPolicy::IntermediateKeyType,
                     ValueVectorType*>(key, vector));
    }
    else
      kvpLookup->second->push_back(value);
  }

  inline void Shuffle() {
    m_drainData = new VectorOfTupplesType();

    for (InputTypeIterator iter = m_data->begin();
         iter != m_data->end();
         iter++)
      m_drainData->push_back(TupleType(iter->first, iter->second));

    m_currentIndex = 0;
    m_maxIndex = m_drainData->size() - 1;
  }

  inline int Size() {
    return m_drainData->size();
  }

  // Lockless, thread safe
  inline bool GetData(typename MapPolicy::IntermediateKeyType& key,
                      ValueVectorIteratorType& iterStart,
                      ValueVectorIteratorType& iterEnd) {
    int thisIndex = std::atomic_fetch_add(&m_currentIndex, 1);

    if (thisIndex > m_maxIndex)
      return false;

    key = (*m_drainData)[thisIndex].Key;
    iterStart = (*m_drainData)[thisIndex].Value->begin();
    iterEnd = (*m_drainData)[thisIndex].Value->end();
    return true;
  }

 private:
  // Source
  InputType* m_data;
  std::mutex m_mutex;

  // Drain
  VectorOfTupplesType* m_drainData;
  std::atomic<int> m_currentIndex;
  int m_maxIndex;
};

} // namespace DataStore
} // namespace MapReduce
} // namespace Thilenius
