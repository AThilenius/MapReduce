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

class JobScheduler
{
public:

	template<
		typename Job
	>
	inline void RunJob(Job& job)
	{
		// Map it
		_MapThread<typename Job>(&job);

		// Reduce it
		_ReduceThread<typename Job>(&job);
	}
};

} // namespace MapReduce
} // namespace Thilenius