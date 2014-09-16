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

	StdMapDrain(InputType* destinationMap) :
		m_dest(destinationMap)
	{
	}

	inline void AddData(typename ReducePolicy::KeyType key, typename ReducePolicy::ValueType value)
	{
		std::lock_guard<std::mutex> guard(m_destMutex);
		m_dest->insert(std::pair<
			typename ReducePolicy::KeyType, 
			typename ReducePolicy::ValueType>(key, value));
	}

private:
	InputType* m_dest;
	std::mutex m_destMutex;
};

} // namespace DataDrain
} // namespace MapReduce
} // namespace Thilenius