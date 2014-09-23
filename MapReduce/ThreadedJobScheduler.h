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
void _ReduceThread(Job* job)
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
		std::cout << "Mapping on " << coresCount << " threads." << std::endl;
		for (int i = 0; i < coresCount; i++)
			threads[i] = std::thread(_MapThread<Job>, &job);

		for (int i = 0; i < coresCount; i++)
			threads[i].join();
		std::cout << "Done Mapping" << std::endl;

		// Shuffle
		std::cout << "Shuffling" << std::endl;
		job.Shuffle();
		std::cout << "Done Shuffling" << std::endl;

		// Reduce
		std::cout << "Reducing on " << coresCount << " threads." << std::endl;
		for (int i = 0; i < coresCount; i++)
			threads[i] = std::thread(_ReduceThread<Job>, &job);

		for (int i = 0; i < coresCount; i++)
			threads[i].join();
		std::cout << "Done Reducing" << std::endl;

		delete[] threads;
	}

};

} // namespace MapReduce
} // namespace Thilenius