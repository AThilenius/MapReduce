//
//  NullReducer.h
//
//  Created by Alec Thilenius on 9/18/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once
#include <unordered_map>
#include <mutex>

namespace Thilenius {
namespace MapReduce { 
namespace Reducers { 

template <
	typename MapPolicy
>
class SelectFirstReducer
{
public:
	typedef typename MapPolicy::IntermediateKeyType		KeyType;
	typedef typename MapPolicy::IntermediateValueType	ValueType;

	template<
		typename Runner,
		typename ItorType
	>
	void Reduce(Runner& runner, KeyType& key, ItorType& begin, ItorType& end)
	{
		if(begin != end)
			runner.Emit(key, *begin);
	}
};

} // namespace DataStore
} // namespace MapReduce
} // namespace Thilenius