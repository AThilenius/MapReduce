//
//  JobScheduler.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once

#include "Job.h"

namespace Thilenius {
namespace MapReduce { 

template<
	typename Job
>
void _MapTask(Job* job)
{
	typename Job::MapPolicyType mapClass;
	typename Job::MapPolicyType::KeyType sourceKey;
	typename Job::MapPolicyType::ValueType sourceValue;
	Job::MapTaskRunner* runner = new Job::MapTaskRunner();

	while (job->Source->GetData(sourceKey, sourceValue))
		mapClass.Map<typename Job::MapTaskRunner>(*runner, sourceKey, sourceValue);

	// Combine results back into primary buffer
	job->DataBuffer->Combine(&runner->LocalBuffer);
	delete runner;
}

template<
	typename Job
>
void _ReduceTask(Job* job)
{
	typename Job::ReducePolicyType reduceClass;
	typename Job::MapPolicyType::IntermediateKeyType intermitKey;
	typename Job::ReduceIteratorType iterStart;
	typename Job::ReduceIteratorType iterEnd;
	Job::ReduceTaskRunner* runner = new Job::ReduceTaskRunner();

	while (job->DataBuffer->GetData(intermitKey, iterStart, iterEnd))
		reduceClass.Reduce<typename Job::ReduceTaskRunner, typename Job::ReduceIteratorType>(*runner, intermitKey, iterStart, iterEnd);

	job->Drain->Combine(&runner->LocalBuffer);
	delete runner;
}

class JobScheduler
{
public:

	template<
		typename Job
	>
	inline void RunJob(Job& job)
	{
		// Map it
		_MapTask<typename Job>(&job);

		// Shuffle it
		job.Shuffle();

		// Reduce it
		_ReduceTask<typename Job>(&job);
	}
};

} // namespace MapReduce
} // namespace Thilenius