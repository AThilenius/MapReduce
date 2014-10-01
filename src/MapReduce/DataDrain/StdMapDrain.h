//
//  StdMapDrain.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once
#include <unordered_map>
#include <mutex>

namespace Thilenius {
namespace MapReduce { 
namespace DataDrain { 

template<
	typename ReducePolicy
>
class StdMapDrain
{
public:
	typedef std::unordered_map<
		typename ReducePolicy::KeyType, 
		typename ReducePolicy::ValueType
	>
	InputType;

	StdMapDrain() :
		m_data(new InputType())
	{
	}

	StdMapDrain(InputType* destinationMap) :
		m_data(destinationMap)
	{
	}

	// Access from any thread
	inline void Combine(StdMapDrain<ReducePolicy>* other)
	{
		std::lock_guard<std::mutex> guard(m_mutex);
		m_data->insert (other->m_data->begin(), other->m_data->end()); 
	}

	// Access from single thread only
	inline void AddData(typename ReducePolicy::KeyType key, typename ReducePolicy::ValueType value)
	{
		m_data->insert(std::pair<
			typename ReducePolicy::KeyType, 
			typename ReducePolicy::ValueType>(key, value));
	}

private:
	InputType* m_data;
	std::mutex m_mutex;
};

} // namespace DataDrain
} // namespace MapReduce
} // namespace Thilenius