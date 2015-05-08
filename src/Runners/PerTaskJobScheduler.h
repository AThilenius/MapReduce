//
//  PerTaskJobScheduler.h
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
    void _MapThreadPerTask(Job* job) {
      RunStatistic* runStat = job->RunStatistics->BeginMap();

      typename Job::MapPolicyType mapClass;
      typename Job::MapPolicyType::KeyType sourceKey;
      typename Job::MapPolicyType::ValueType sourceValue;

      typename Job::MapTaskRunner* runner = new typename Job::MapTaskRunner();

      if (job->Source->GetData(sourceKey, sourceValue)) {
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
    void _ReduceThreadPerTask(Job* job) {
      RunStatistic* reduceStat = job->RunStatistics->BeginReduce();

      typename Job::ReducePolicyType reduceClass;
      typename Job::MapPolicyType::IntermediateKeyType intermitKey;
      typename Job::ReduceIteratorType iterStart;
      typename Job::ReduceIteratorType iterEnd;

      typename Job::ReduceTaskRunner* runner
          = new typename Job::ReduceTaskRunner(job->Drain);

      if (job->DataBuffer->GetData(intermitKey, iterStart, iterEnd)) {
        reduceStat->BeginTask();

        reduceClass.template Reduce<
            typename Job::ReduceTaskRunner,
                     typename Job::ReduceIteratorType
                         >(*runner, intermitKey, iterStart, iterEnd);

        reduceStat->EndTask();
      }

      delete runner;
      reduceStat->EndTaskCall();
    }

class PerTaskJobScheduler {
 public:
  template<typename Job>
      inline void RunJob(Job& job) {
        job.RunStatistics->BeginJob();

        // Map
        {
          int size = job.Source->Size();
          std::vector<std::thread> threads;
          for (int i = 0; i < size; i++) {
            threads.push_back(std::thread(_MapThreadPerTask<Job>, &job));
          }
          // Wait on all mapper threads
          for (int i = 0; i < size; i++) {
            threads[i].join();
          }
        }

        // Shuffle
        job.Shuffle();

        // Reduce
        {
          int size = job.DataBuffer->Size();
          std::vector<std::thread> threads;
          for (int i = 0; i < size; i++) {
            threads.push_back(std::thread(_ReduceThreadPerTask<Job>, &job));
          }
          // Wait on all reducer threads
          for (int i = 0; i < size; i++) {
            threads[i].join();
          }

        }

        job.RunStatistics->EndJob();
      }
};

} // namespace MapReduce
} // namespace Thilenius
