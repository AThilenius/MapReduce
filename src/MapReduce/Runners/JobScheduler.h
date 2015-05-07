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

template<typename Job>
    void _MapTask(Job* job) {
      RunStatistic* runStat = job->RunStatistics->BeginMap();

      typename Job::MapPolicyType mapClass;
      typename Job::MapPolicyType::KeyType sourceKey;
      typename Job::MapPolicyType::ValueType sourceValue;

      typename Job::MapTaskRunner* runner = new typename Job::MapTaskRunner();

      while (job->Source->GetData(sourceKey, sourceValue)) {
        runStat->BeginTask();

        mapClass.template Map<typename Job::MapTaskRunner>
            (*runner, sourceKey, sourceValue);

        runStat->EndTask();
      }

      // Combine results back into primary buffer
      job->DataBuffer->Combine(&runner->LocalBuffer);
      delete runner;

      runStat->EndTaskCall();
    }

template<typename Job>
    void _ReduceTask(Job* job) {
      RunStatistic* reduceStat = job->RunStatistics->BeginReduce();

      typename Job::ReducePolicyType reduceClass;
      typename Job::MapPolicyType::IntermediateKeyType intermitKey;
      typename Job::ReduceIteratorType iterStart;
      typename Job::ReduceIteratorType iterEnd;

      typename Job::ReduceTaskRunner* runner
          = new typename Job::ReduceTaskRunner(job->Drain);

      while (job->DataBuffer->GetData(intermitKey, iterStart, iterEnd)) {
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

class JobScheduler {
 public:
  template<typename Job>
      inline void RunJob(Job& job) {
        job.RunStatistics->BeginJob();

        // Map it
        _MapTask<Job>(&job);

        // Shuffle it
        job.Shuffle();

        // Reduce it
        _ReduceTask<Job>(&job);

        job.RunStatistics->EndJob();
      }
};

} // namespace MapReduce
} // namespace Thilenius
