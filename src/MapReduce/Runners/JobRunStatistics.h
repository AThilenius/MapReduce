//
//  JobRunStatistics.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//
#pragma once

#include <chrono>
#include <limits>
#include <thread>

#define TO_SEC(val) ((double)val / 1000000.0L)

namespace Thilenius {
namespace MapReduce {

class RunStatistic {
 public:
  RunStatistic() :
    RunTimeNs(-1),
    m_runStartTime(std::chrono::high_resolution_clock::now()),
    m_lastTaskBeginTime(std::chrono::high_resolution_clock::now()) {
  }

  inline void EndTaskCall() {
    auto endTime = std::chrono::high_resolution_clock::now();
    RunTimeNs = std::chrono::duration_cast<std::chrono::microseconds>(
        endTime - m_runStartTime).count();
  }

  inline void BeginTask() {
    m_lastTaskBeginTime = std::chrono::high_resolution_clock::now();
  }

  inline void EndTask() {
    auto endTime = std::chrono::high_resolution_clock::now();
    int time = std::chrono::duration_cast<std::chrono::microseconds>(
        endTime - m_lastTaskBeginTime).count();

    TaskTimesMicroS.push_back(time);
  }

 public:
  std::vector<int> TaskTimesMicroS;
  int RunTimeNs;

 private:
  typedef std::chrono::high_resolution_clock::time_point Time;
  Time m_runStartTime;
  Time m_lastTaskBeginTime;
};

class JobRunStatistics {
 public:
  JobRunStatistics() :
    m_jobRunTimeMicroS(-1) {
  }

  inline RunStatistic* BeginMap() {
    std::lock_guard<std::mutex> guard(m_mutex);

    RunStatistic* runStat = new RunStatistic();
    m_mapRuns.push_back(runStat);
    return runStat;
  }

  inline RunStatistic* BeginReduce() {
    std::lock_guard<std::mutex> guard(m_mutex);

    RunStatistic* runStat = new RunStatistic();
    m_reduceRuns.push_back(runStat);
    return runStat;
  }

  inline void BeginJob() {
    m_jobStartTime = std::chrono::high_resolution_clock::now();
  }

  inline void EndJob() {
    auto endTime = std::chrono::high_resolution_clock::now();
    m_jobRunTimeMicroS = std::chrono::duration_cast
        <std::chrono::microseconds>(endTime - m_jobStartTime).count();
  }

  inline void BeginShuffle() {
    m_shuffleStartTime = std::chrono::high_resolution_clock::now();
  }

  inline void EndShuffle() {
    auto endTime = std::chrono::high_resolution_clock::now();
    m_shuffleRunTimeMicroS = std::chrono::duration_cast
        <std::chrono::microseconds>(endTime - m_shuffleStartTime).count();
  }

  inline void PrintStatistics() {
    double jobDurration = TO_SEC(m_jobRunTimeMicroS);
    double shuffleDurration = TO_SEC(m_shuffleRunTimeMicroS);
    unsigned coresCount = std::thread::hardware_concurrency();

    std::cout << "Job:" << std::endl;
    std::cout << "  - Wall Run Time:      " << jobDurration << std::endl;
    std::cout << "  - Avalable Cores:     " << coresCount << std::endl;
    std::cout << "\nMap:" << std::endl;
    PrintRunStats(m_mapRuns);
    std::cout << "\nShuffle:" << std::endl;
    std::cout << "  - Wall Run Time:      " << shuffleDurration
              << std::endl;
    std::cout << "\nReduce:" << std::endl;
    PrintRunStats(m_reduceRuns);
  }

 private:
  inline void PrintRunStats(std::vector<RunStatistic*>& runs) {
    // Per Runner
    double minRun = std::numeric_limits<double>::max();
    double maxRun = std::numeric_limits<double>::min();
    double totalRunTime = 0.0;
    double avgRunTime = -1.0;

    // Per-Task
    int taskcount = 0;
    double minTask = std::numeric_limits<double>::max();
    double maxTask = std::numeric_limits<double>::min();
    double totalTaskRunTime = 0.0;
    double avgTaskRunTime = -1.0;

    // Foreach Runner:
    for (int i = 0; i < runs.size(); i++) {
      double runTime = TO_SEC(runs[i]->RunTimeNs);
      minRun = (runTime < minRun) ? runTime : minRun;
      maxRun = (runTime > maxRun) ? runTime : maxRun;
      totalRunTime += runTime;

      // Foreach task:
      for (int t = 0; t < runs[i]->TaskTimesMicroS.size(); t++) {
        taskcount++;
        double taskTime = TO_SEC(runs[i]->TaskTimesMicroS[t]);
        minTask = (taskTime < minRun) ? taskTime : minRun;
        maxTask = (taskTime > maxRun) ? taskTime : maxRun;
        totalTaskRunTime += taskTime;
      }
    }

    avgRunTime = totalRunTime / (double) runs.size();
    avgTaskRunTime = totalTaskRunTime / (double) taskcount;

    std::cout << "  - Runners (Threads):  " << runs.size() << std::endl;
    std::cout << "    - Total Run Time:   " << totalRunTime << std::endl;
    std::cout << "    - Average Run Time: " << avgRunTime << std::endl;
    std::cout << "    - Minimum Run Time: " << minRun << std::endl;
    std::cout << "    - Maximum Run Time: " << maxRun << std::endl;
    std::cout << "  - Tasks: " << std::endl;
    std::cout << "    - Count:            " << taskcount << std::endl;
    std::cout << "    - Average Run Time: " << avgTaskRunTime << std::endl;
    std::cout << "    - Minimum Run Time: " << minTask << std::endl;
    std::cout << "    - Maximum Run Time: " << maxTask << std::endl;
  }

 private:
  typedef std::chrono::high_resolution_clock::time_point Time;

  std::mutex m_mutex;
  Time m_jobStartTime;
  Time m_shuffleStartTime;
  int m_jobRunTimeMicroS;
  int m_shuffleRunTimeMicroS;
  std::vector<RunStatistic*> m_mapRuns;
  std::vector<RunStatistic*> m_reduceRuns;
};

} // namespace MapReduce
} // namespace Thilenius
