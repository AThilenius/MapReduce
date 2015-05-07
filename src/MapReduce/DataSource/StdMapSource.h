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
public:
	typedef std::unordered_map<
		typename MapPolicy::KeyType, 
		typename MapPolicy::ValueType
	>
	InputType;

	typedef typename std::unordered_map<
		typename MapPolicy::KeyType, 
		typename MapPolicy::ValueType
	>::iterator
	InputTypeIterator;

	StdMapSource(InputType* SourceMap):
		m_dataSource(SourceMap)
	{
	}
    
    inline int Size()
    {
        return m_dataSource->size();
    }

	inline bool GetData(typename MapPolicy::KeyType& key,
                        typename MapPolicy::ValueType& value)
	{
		std::lock_guard<std::mutex> guard(m_sourceMutex);
		InputTypeIterator begin = m_dataSource->begin();
		InputTypeIterator end = m_dataSource->end();
		if (begin == end)
			return false;

		key = begin->first;
		value = begin->second;
		m_dataSource->erase(begin);
		return true;
	}

private:
	InputType* m_dataSource;
	std::mutex m_sourceMutex;
};

} // namespace DataStore
} // namespace MapReduce
} // namespace Thilenius