//
//  ThreadedJobScheduler.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once

#include <thread>
#include <iostream>

#include "Job.h"

namespace Thilenius {
namespace MapReduce { 

template<
	typename Job
>
void _MapThread(Job* job)
{
	typename Job::MapPolicyType mapClass;
	typename Job::MapPolicyType::KeyType sourceKey;
	typename Job::MapPolicyType::ValueType sourceValue;
    
	typename Job::MapTaskRunner* runner = new typename Job::MapTaskRunner();

	while (job->Source->GetData(sourceKey, sourceValue))
		mapClass.template Map<typename Job::MapTaskRunner>(*runner, sourceKey, sourceValue);

	// Combine results back into primary buffer
	job->DataBuffer->Combine(&runner->LocalBuffer);
	delete runner;
}

template<
	typename Job
>
void _ReduceThread(Job* job)
{
	typename Job::ReducePolicyType reduceClass;
	typename Job::MapPolicyType::IntermediateKeyType intermitKey;
	typename Job::ReduceIteratorType iterStart;
	typename Job::ReduceIteratorType iterEnd;
    
	typename Job::ReduceTaskRunner* runner = new typename Job::ReduceTaskRunner();

	while (job->DataBuffer->GetData(intermitKey, iterStart, iterEnd))
		reduceClass.template Reduce<typename Job::ReduceTaskRunner, typename Job::ReduceIteratorType>(*runner, intermitKey, iterStart, iterEnd);

	job->Drain->Combine(&runner->LocalBuffer);
	delete runner;
}

class ThreadedJobScheduler
{
public:
	template<
		typename Job
	>
	inline void RunJob(Job& job)
	{
		unsigned coresCount = std::thread::hardware_concurrency();

		if (coresCount < 2)
			coresCount = 4;

		std::thread* threads = new std::thread[coresCount];

		// Map
		for (int i = 0; i < coresCount; i++)
			threads[i] = std::thread(_MapThread<Job>, &job);

		for (int i = 0; i < coresCount; i++)
			threads[i].join();

		// Shuffle
		job.Shuffle();

		// Reduce
		for (int i = 0; i < coresCount; i++)
			threads[i] = std::thread(_ReduceThread<Job>, &job);

		for (int i = 0; i < coresCount; i++)
			threads[i].join();

		delete[] threads;
	}

};

} // namespace MapReduce
} // namespace Thilenius