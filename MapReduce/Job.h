//
//  Job.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once

#include "StdMapSource.h"
#include "FileLoaderSource.h"
#include "StdMapDrain.h"
#include "StdMapVectorBuffer.h"

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

	while (job->m_source->GetData(sourceKey, sourceValue))
		mapClass.Map<typename Job::MapTaskRunner>(*runner, sourceKey, sourceValue);

	// Combine results back into primary buffer
	job->m_dataBuffer->Combine(&runner->m_localBuffer);
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

	while (job->m_dataBuffer->GetData(intermitKey, iterStart, iterEnd))
		reduceClass.Reduce<typename Job::ReduceTaskRunner, typename Job::ReduceIteratorType>(*runner, intermitKey, iterStart, iterEnd);

	job->m_drain->Combine(&runner->m_localBuffer);
	delete runner;
}


template<
	typename MapPolicy,
	typename ReducePolicy,
	typename SourcePolicy = DataSource::FileLoaderSource<MapPolicy>,
	typename DrainPolicy = DataDrain::StdMapDrain<ReducePolicy>,
	typename BufferPolicy = DataBuffer::StdMapVectorBuffer<MapPolicy>
>
class Job
{
public:
	typedef typename MapPolicy MapPolicyType;
	typedef typename ReducePolicy ReducePolicyType;
	typedef typename SourcePolicy::InputType SourceInputType;
	typedef typename DrainPolicy::InputType DrainInputType;
	typedef typename BufferPolicy::ReduceIterator ReduceIteratorType;

	class MapTaskRunner
	{
	public:
		inline void Emit(typename MapPolicy::IntermediateKeyType key, typename MapPolicy::IntermediateValueType value)
		{
			m_localBuffer.AddData(key, value);
		}

	private:
		BufferPolicy m_localBuffer;
		template<typename J>
		friend void _MapThread(J* job); 
	};

	class ReduceTaskRunner
	{
	public:
		inline void Emit(typename ReducePolicy::KeyType key, typename ReducePolicy::ValueType value)
		{
			m_localBuffer.AddData(key, value);
		}

	private:
		friend class Job;

	private:
		DrainPolicy m_localBuffer;
		template<typename J>
		friend void _ReduceThread(J* job);
	};

	Job(SourceInputType& source, DrainInputType& drain) :
		m_dataBuffer(new BufferPolicy()),
		m_source(new SourcePolicy(&source)),
		m_drain(new DrainPolicy(&drain))
	{
	}

	~Job()
	{
		SAFE_FREE(m_dataBuffer);
		SAFE_FREE(m_source);
		SAFE_FREE(m_drain);
	}

private:
	// Owned
	BufferPolicy* m_dataBuffer;
	SourcePolicy* m_source;
	DrainPolicy* m_drain;

	template<typename J>
	friend void _MapThread(J* job);

	template<typename J>
	friend void _ReduceThread(J* job);
};

} // namespace MapReduce
} // namespace Thilenius