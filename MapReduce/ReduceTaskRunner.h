//
//  ReduceTaskRunner.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once

namespace Thilenius {
namespace MapReduce {

template<
	typename ReducePolicy
>
class ReduceTaskRunner
{
public:
	ReduceTaskRunner();
	~ReduceTaskRunner();

	inline void Emit(typename ReducePolicy::KeyType& key, typename ReducePolicy::ValueType& value)
	{

	}

};

} // namespace MapReduce
} // namespace Thilenius