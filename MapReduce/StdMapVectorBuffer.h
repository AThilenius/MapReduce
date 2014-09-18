//
//  StdMapBuffer.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once
#include <unordered_map>
#include <mutex>
#include <vector>

namespace Thilenius {
namespace MapReduce { 
namespace DataBuffer { 

template<
	typename MapPolicy
>
class StdMapVectorBuffer
{
public:
	typedef std::vector<
		typename MapPolicy::IntermediateValueType
	>
	VectorType;

	typedef std::unordered_map<
		typename MapPolicy::IntermediateKeyType, 
		VectorType*
	>
	InputType;

	typedef typename std::unordered_map<
		typename MapPolicy::IntermediateKeyType, 
		VectorType*
	>::iterator
	InputTypeIterator;

	typedef typename std::unordered_map<
		typename MapPolicy::IntermediateKeyType, 
		VectorType*
	>::const_iterator
	InputTypeConstIterator;

	typedef typename VectorType::iterator
	ReduceIterator;

	StdMapVectorBuffer():
		m_data(new InputType())
	{
	}

	~StdMapVectorBuffer()
	{
		SAFE_FREE(m_data);
	}

	// Thread Safe
	inline void Combine(typename StdMapVectorBuffer<typename MapPolicy>* other)
	{
		std::lock_guard<std::mutex> guard(m_mutex);
		m_data->insert (other->m_data->begin(), other->m_data->end()); 
	}

	inline void AddData(typename MapPolicy::IntermediateKeyType key, typename MapPolicy::IntermediateValueType value)
	{
		InputTypeConstIterator kvpLookup = m_data->find(key);

		if (kvpLookup == m_data->end())
		{
			VectorType* vector = new VectorType();
			vector->push_back(value);
			m_data->insert(std::pair<
				typename MapPolicy::IntermediateKeyType,
				VectorType*>(key, vector));
		}
		else
			kvpLookup->second->push_back(value);
	}

	// Thread Safe
	inline bool GetData(typename MapPolicy::IntermediateKeyType& key, ReduceIterator& iterStart, ReduceIterator& iterEnd)
	{
		std::lock_guard<std::mutex> guard(m_mutex);
		InputTypeIterator begin = m_data->begin();
		InputTypeIterator end = m_data->end();
		if (begin == end)
			return false;

		key = begin->first;
		iterStart = begin->second->begin();
		iterEnd = begin->second->end();
		m_data->erase(begin);
		return true;
	}

private:
	InputType* m_data;
	std::mutex m_mutex;
};

} // namespace DataStore
} // namespace MapReduce
} // namespace Thilenius