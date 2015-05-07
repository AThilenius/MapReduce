//
//  Main.cpp
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//
#include "stdafx.h"

#include "Job.h"
#include "JobScheduler.h"
#include "PerCoreJobScheduler.h"
#include "PerTaskJobScheduler.h"

class MapTask;
class ReduceTask;

using namespace Thilenius::MapReduce;

typedef Thilenius::MapReduce::Job<
  MapTask,
  ReduceTask,
  DataSource::FileLoaderSource<MapTask>,
  DataDrain::FileDrain<ReduceTask>,
  DataBuffer::StdMapVectorBuffer<MapTask>
> DNSLookupJob;

class MapTask {
 public:
  typedef std::string KeyType;
  typedef std::ifstream* ValueType;
  typedef std::string IntermediateKeyType;
  typedef int IntermediateValueType;

  template<typename Runner>
  void Map(Runner& runner, KeyType& key, ValueType& value) {
    std::string word;
    while ((*value) >> word) {
      runner.Emit(word, 1);
    }
  }
};

class ReduceTask {
 public:
  typedef std::string KeyType;
  typedef int ValueType;

  template<typename Runner, typename ItorType>
  void Reduce(Runner& runner, KeyType& key, ItorType& begin, ItorType& end) {
    int total = 0;
    for(; begin != end; begin++)
      total++;
    runner.Emit(key, total);
  }
};

int main(int argc, char* argv[]) {
  std::cout << "Beginning MapReduce Job" << std::endl;
  std::vector<std::string> inputFiles(argv + 1, argv + (argc - 1));
  std::string outputFile(argv[argc - 1]);

  DNSLookupJob job(inputFiles, outputFile);
  Thilenius::MapReduce::PerCoreJobScheduler scheduler;
  scheduler.RunJob(job);
  job.RunStatistics->PrintStatistics();
}
