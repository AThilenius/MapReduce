//
//  StdMapBuffer.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once
#include <unordered_map>
#include <mutex>

namespace Thilenius {
namespace MapReduce { 
namespace DataBuffer { 

template<
	typename MapPolicy
>
class StdMapBuffer
{
public:
	typedef std::unordered_map<
		typename MapPolicy::IntermediateKeyType, 
		typename MapPolicy::IntermediateValueType
	>
	InputType;

	typedef typename std::unordered_map<
		typename MapPolicy::IntermediateKeyType, 
		typename MapPolicy::IntermediateValueType
	>::iterator
	InputTypeIterator;

	StdMapBuffer():
		m_data(new InputType())
	{
	}

	~StdMapBuffer()
	{
		SAFE_FREE(m_data);
	}

	inline void AddData(typename MapPolicy::IntermediateKeyType& key, typename MapPolicy::IntermediateValueType& value)
	{
		std::lock_guard<std::mutex> guard(m_dataMutex);
		m_data->insert(std::make_pair<
			typename ReducePolicy::KeyType, 
			typename ReducePolicy::ValueType>(key, value));
	}

	inline bool GetData(typename MapPolicy::IntermediateKeyType& key, typename MapPolicy::IntermediateValueType& value)
	{
		std::lock_guard<std::mutex> guard(m_dataMutex);
		InputTypeIterator begin = m_data->begin();
		InputTypeIterator end = m_data->end();
		if (begin == end)
			return false;

		key = begin->first;
		value = begin->second;
		m_data->erase(begin);
		return true;
	}

private:
	InputType* m_data;
	std::mutex m_dataMutex;
};

} // namespace DataStore
} // namespace MapReduce
} // namespace Thilenius