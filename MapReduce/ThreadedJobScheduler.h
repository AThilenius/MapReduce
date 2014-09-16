//
//  ThreadedJobScheduler.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once

#include <thread>

#include "Job.h"

namespace Thilenius {
namespace MapReduce { 


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