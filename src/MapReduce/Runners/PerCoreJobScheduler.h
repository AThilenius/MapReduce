//
//  PerCoreJobScheduler.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//
#pragma once

#include <iostream>
#include <thread>

#include "Job.h"

namespace Thilenius {
namespace MapReduce {

template<typename Job>
    void _MapThread(Job* job) {
      RunStatistic* runStat = job->RunStatistics->BeginMap();

      typename Job::MapPolicyType mapClass;
      typename Job::MapPolicyType::KeyType sourceKey;
      typename Job::MapPolicyType::ValueType sourceValue;

      typename Job::MapTaskRunner* runner = new typename Job::MapTaskRunner();

      while (job->Source->GetData(sourceKey, sourceValue)) {
        runStat->BeginTask();
        mapClass.template Map<
            typename Job::MapTaskRunner
            >(*runner, sourceKey, sourceValue);
        runStat->EndTask();
      }

      // Combine results back into primary buffer
      job->DataBuffer->Combine(&runner->LocalBuffer);
      delete runner;

      runStat->EndTaskCall();
    }

template<typename Job>
    void _ReduceThread(Job* job) {
      RunStatistic* reduceStat = job->RunStatistics->BeginReduce();

      typename Job::ReducePolicyType reduceClass;
      typename Job::MapPolicyType::IntermediateKeyType intermitKey;
      typename Job::ReduceIteratorType iterStart;
      typename Job::ReduceIteratorType iterEnd;

      typename Job::ReduceTaskRunner* runner
          = new typename Job::ReduceTaskRunner(job->Drain);

      while (job->DataBuffer->GetData(intermitKey, iterStart, iterEnd)) {
        reduceStat->BeginTask();
        reduceClass.template Reduce<typename Job::ReduceTaskRunner,
            typename Job::ReduceIteratorType>(*runner, intermitKey, iterStart,
                                              iterEnd);
        reduceStat->EndTask();
      }

      delete runner;

      reduceStat->EndTaskCall();
    }

class PerCoreJobScheduler {
 public:
  template<typename Job>
      inline void RunJob(Job& job) {
        job.RunStatistics->BeginJob();
        unsigned coresCount = std::thread::hardware_concurrency();
        if (coresCount < 2) {
          coresCount = 4;
        }
        std::thread* threads = new std::thread[coresCount];

        // Map
        for (int i = 0; i < coresCount; i++) {
          threads[i] = std::thread(_MapThread<Job>, &job);
        }
        for (int i = 0; i < coresCount; i++) {
          threads[i].join();
        }

        // Shuffle
        job.Shuffle();

        // Reduce
        for (int i = 0; i < coresCount; i++) {
          threads[i] = std::thread(_ReduceThread<Job>, &job);
        }
        for (int i = 0; i < coresCount; i++) {
          threads[i].join();
        }
        delete[] threads;
        job.RunStatistics->EndJob();
      }
};

} // namespace MapReduce
} // namespace Thilenius
