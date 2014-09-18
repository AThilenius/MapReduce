//
//  DirectoryLoaderSource.h
//
//  Created by Alec Thilenius on 9/18/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once
#include <type_traits>
#include <mutex>
#include <string>

namespace Thilenius {
namespace MapReduce { 
namespace DataSource { 

template<
	typename MapPolicy
>
class DirectoryLoaderSource
{
public:
	typedef std::string InputType;

	DirectoryLoaderSource(InputType* directoryPath)
	{
	}

	inline bool GetData(typename MapPolicy::KeyType& key, typename MapPolicy::ValueType& value)
	{
		
		return true;
	}

private:
	std::mutex m_sourceMutex;
};

} // namespace DataStore
} // namespace MapReduce
} // namespace Thilenius