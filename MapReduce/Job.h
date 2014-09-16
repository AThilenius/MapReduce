//
//  Job.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once

#include "MapTaskRunner.h"
#include "ReduceTaskRunner.h"
#include "StdMapSource.h"
#include "StdMapDrain.h"

namespace Thilenius {
namespace MapReduce { 

template<
	typename MapPolicy,
	typename ReducePolicy,
	typename SourcePolicy = DataSource::StdMapSource<MapPolicy>,
	typename DrainPolicy = DataDrain::StdMapDrain<ReducePolicy>
>
class Job
{
public:
	typedef MapTaskRunner<MapPolicy> MapTaskRunner;
	typedef ReduceTaskRunner<ReducePolicy> ReduceTaskRunner;

	// Runs the full map reduce
	//void Run(SourcePolicy& source, DrainPolicy& drain)
	//{
	//	typename MapPolicy::KeyType& key;
	//	typename MapPolicy::ValueType& value;
	//	while (source.GetData(key, value))
	//		MapPolicy::Map(m_mapRunner, key, value)
	//}

	//MapTaskRunner m_mapRunner;
	//ReduceTaskRunner m_reduceRunner;
};

} // namespace MapReduce
} // namespace Thilenius