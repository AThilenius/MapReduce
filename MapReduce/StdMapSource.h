//
//  StdMapSource.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once
#include <unordered_map>
#include <mutex>

namespace Thilenius {
namespace MapReduce { 
namespace DataSource { 

template<
	typename MapPolicy
>
class StdMapSource
{
	typedef std::unordered_map<
		typename MapPolicy::KeyType, 
		typename MapPolicy::ValueType
	>
	InputMapType;

	typedef typename std::unordered_map<
		typename MapPolicy::KeyType, 
		typename MapPolicy::ValueType
	>::iterator
	InputMapTypeIterator;

public:
	StdMapSource(InputMapType* SourceMap):
		m_dataSource(SourceMap),
		m_iter(SourceMap->begin()),
		m_endIter(SourceMap->end())
	{
	}

	inline bool GetData(typename MapPolicy::KeyType& key, typename MapPolicy::ValueType& value)
	{
		m_sourceMutex.lock();
		InputMapTypeIterator being = m_dataSource->begin();
		InputMapTypeIterator end = m_dataSource->end();
		if (begin == end)
		{
			m_sourceMutex.unlock();
			return false;
		}

		key = being->first;
		value = being->second;
		m_dataSource->erase(begin);

		m_sourceMutex.unlock();
		return true;
	}

private:
	InputMapType* m_dataSource;
	std::mutex m_sourceMutex;
};

} // namespace DataStore
} // namespace MapReduce
} // namespace Thilenius