//
//  Job.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//
#pragma once

#include "FileDrain.h"
#include "FileLoaderSource.h"
#include "JobRunStatistics.h"
#include "StdMapDrain.h"
#include "StdMapSource.h"
#include "StdMapVectorBuffer.h"

namespace Thilenius {
namespace MapReduce {

template<typename MapPolicy, typename ReducePolicy,
    typename SourcePolicy = DataSource::FileLoaderSource<MapPolicy>,
    typename DrainPolicy = DataDrain::FileDrain<ReducePolicy>,
    typename BufferPolicy = DataBuffer::StdMapVectorBuffer<MapPolicy>>
        class Job {
         public:
          typedef MapPolicy MapPolicyType;
          typedef ReducePolicy ReducePolicyType;
          typedef typename SourcePolicy::InputType SourceInputType;
          typedef typename DrainPolicy::InputType DrainInputType;
          typedef typename BufferPolicy::ValueVectorIteratorType
              ReduceIteratorType;

          class MapTaskRunner {
           public:
            inline void Emit(typename MapPolicy::IntermediateKeyType key,
                             typename MapPolicy::IntermediateValueType value) {
              LocalBuffer.AddData(key, value);
            }

            BufferPolicy LocalBuffer;
          };

          class ReduceTaskRunner {
           public:
            ReduceTaskRunner(DrainPolicy* drain) :
              m_drain(drain) {

              }

            inline void Emit(typename ReducePolicy::KeyType key,
                             typename ReducePolicy::ValueType value) {
              m_drain->AddData(key, value);
            }

           private:
            DrainPolicy* m_drain;
            friend class Job;
          };

          Job(SourceInputType& source, DrainInputType& drain) :
            DataBuffer(new BufferPolicy()),
            Source(new SourcePolicy(&source)),
            Drain(new DrainPolicy(&drain)),
            RunStatistics(new JobRunStatistics()) {

          }

          ~Job() {
            SAFE_FREE(DataBuffer);
            SAFE_FREE(Source);
            SAFE_FREE(Drain);
            SAFE_FREE(RunStatistics);
          }

          void Shuffle() {
            RunStatistics->BeginShuffle();
            DataBuffer->Shuffle();
            RunStatistics->EndShuffle();
          }

         public:
          // Owned
          BufferPolicy* DataBuffer;
          SourcePolicy* Source;
          DrainPolicy* Drain;
          JobRunStatistics* RunStatistics;
        };

} // namespace MapReduce
} // namespace Thilenius
