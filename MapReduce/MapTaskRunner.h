//
//  MapTaskRunner.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//
#pragma once

namespace Thilenius {
namespace MapReduce {

template<
	typename MapPolicy
>
class MapTaskRunner
{
public:
	MapTaskRunner();
	~MapTaskRunner();

	inline void Emit(typename MapPolicy::IntermediateKeyType& key, typename MapPolicy::IntermediateValueType& value)
	{

	}
	
};

} // namespace MapReduce
} // namespace Thilenius