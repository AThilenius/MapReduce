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
	typedef std::unordered_map<
		typename ReducePolicy::KeyType, 
		typename ReducePolicy::ValueType
	>
	InputMapType;

public:
	StdMapDrain(InputMapType* destinationMap) :
		m_dest(destinationMap)
	{
	}

	inline void AddData(typename ReducePolicy::KeyType& key, typename ReducePolicy::ValueType& value)
	{
		m_destMutex.lock();
		m_dest->insert(std::make_pair<
			typename ReducePolicy::KeyType, 
			typename ReducePolicy::ValueType>(key, value));
		m_destMutex.unlock();
	}

private:
	InputMapType* m_dest;
	std::mutex m_destMutex;
};

} // namespace DataDrain
} // namespace MapReduce
} // namespace Thilenius