//
//  StackAllocator.h
//
//  Created by Alec Thilenius on 9/26/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once
#include <unordered_map>
#include <mutex>

namespace Thilenius {
namespace MapReduce { 
namespace Memory { 

template<
	typename T
>
class StackAllocator
{
public:

	StackAllocator(size_t size)
	{

	}

	void Reset()
	{

	}

	// Thread-safe, lockless
	T* Malloc (size_t count)
	{

	}

};

} // namespace Memory
} // namespace MapReduce
} // namespace Thilenius