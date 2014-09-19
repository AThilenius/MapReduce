//
//  FileLoaderSource.h
//
//  Created by Alec Thilenius on 9/18/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once
#include <mutex>
#include <string>
#include <vector>
#include <sstream>

namespace Thilenius {
namespace MapReduce { 
namespace DataSource { 

/************************************************************************/
/* Parses a space delimited string of file paths. Opens each file and    /
/* hands out a tuple of <path, ifstream>                                 /
/************************************************************************/
template<
	typename MapPolicy
>
class FileLoaderSource
{
public:
	typedef std::string InputType;

	FileLoaderSource(InputType* directoryPath)
	{
		std::string buf;
		std::stringstream ss(*directoryPath);
		while (ss >> buf)
			m_tokens.push_back(buf);
	}

	inline bool GetData(typename MapPolicy::KeyType& key, typename MapPolicy::ValueType& value)
	{
		m_mutex.lock();

		if (m_tokens.empty())
		{
			m_mutex.unlock();
			return false;
		}

		key = m_tokens.back();
		m_tokens.pop_back();

		m_mutex.unlock();
		
		std::ifstream* stream = new std::ifstream(key.c_str());

		if (!stream)
			throw std::exception("Failed to load file");

		value = stream;
		return true;
	}

private:
	std::vector<std::string> m_tokens;
	std::mutex m_mutex;
};

} // namespace DataStore
} // namespace MapReduce
} // namespace Thilenius