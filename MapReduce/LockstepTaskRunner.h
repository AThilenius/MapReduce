//
//  LockstepTaskRunner.h
//
//  Created by Alec Thilenius on 9/19/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once

#include <thread>
#include <atomic>
#include <functional>

namespace Thilenius {
namespace Threading { 
  
template <typename ArgType>
class LockstepTaskRunner;

template <typename ArgType>
void _TaskThread(LockstepTaskRunner<typename ArgType>& runner);

template <
	typename ArgType
>
class LockstepTaskRunner {
public:
	LockstepTaskRunner(std::function<void(typename ArgType)> function) :
		m_function(function)
	{

	}

	~LockstepTaskRunner()
	{

	}

    void Enqueue(typename ArgType argInstance)
	{
		m_args.push_back(argInstance);
	}

	void RunTaskQueueLockstep(int allocationCount = 64)
	{
		m_currentIndex = 0;

		if (allocationCount > m_args.size())
			allocationCount = m_args.size();

		std::vector<std::thread> threads;

		for (int i = 0; i < allocationCount; i++)
			threads.push_back(std::thread(_TaskThread<typename ArgType>, *this));

		for (int i = 0; i < allocationCount; i++)
			threads[i].join();
	}

private:
	template<typename A>
	friend void _TaskThread(A job);

	// Lockless trackers
	unsigned int m_currentIndex;
	unsigned int m_maxIndex;

	std::function<void(typename ArgType)> m_function;
    std::vector<typename ArgType> m_args;
};

template <
	typename ArgType
>
void _TaskThread(LockstepTaskRunner<typename ArgType>& runner)
{
	while(true)
	{
		int thisItemID = std::atomic::fetch_add(runner.m_currentIndex, 1);
		if (thisItemID == runner.m_maxIndex)
			return;

		// This thread now owns item thisItemID
		runner.m_function(runner.m_args[thisItemID]);
	}
}

} // namespace Threading
} // namespace Thilenius