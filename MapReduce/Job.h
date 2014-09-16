//
//  Job.h
//
//  Created by Alec Thilenius on 9/15/2014.
//  Copyright (c) 2013 Thilenius. All rights reserved.
//

#pragma once

#include "StdMapSource.h"
#include "StdMapDrain.h"
#include "StdMapVectorBuffer.h"

namespace Thilenius {
namespace MapReduce { 


template<
	typename Job
>
void _MapThread(Job* job)
{
	//Job& job = *((Job*) jobPrt);
	typename Job::MapPolicyType mapClass;
	typename Job::MapPolicyType::KeyType sourceKey;
	typename Job::MapPolicyType::ValueType sourceValue;
	while (job->m_source.GetData(sourceKey, sourceValue)) 
		mapClass.Map(*job->m_mapRunner, sourceKey, sourceValue);
}

template<
	typename Job
>
void _ReduceThread(Job* job)
{
	//Job& job = *((Job*) jobPrt);
	typename Job::ReducePolicyType reduceClass;
	typename Job::MapPolicyType::IntermediateKeyType intermitKey;
	typename Job::ReduceIteratorType iterStart;
	typename Job::ReduceIteratorType iterEnd;
	while (job->m_dataBuffer->GetData(intermitKey, iterStart, iterEnd))
		reduceClass.Reduce(*job->m_reduceRunner, intermitKey, iterStart, iterEnd);
}


template<
	typename MapPolicy,
	typename ReducePolicy,
	typename SourcePolicy = DataSource::StdMapSource<MapPolicy>,
	typename DrainPolicy = DataDrain::StdMapDrain<ReducePolicy>,
	typename BufferPolicy = DataBuffer::StdMapVectorBuffer<MapPolicy>
>
class Job
{
public:
	typedef typename MapPolicy MapPolicyType;
	typedef typename ReducePolicy ReducePolicyType;
	typedef typename BufferPolicy::ReduceIterator ReduceIteratorType;

	class MapTaskRunner
	{
	public:
		inline void Emit(typename MapPolicy::IntermediateKeyType key, typename MapPolicy::IntermediateValueType value)
		{
			m_dataBuffer->AddData(key, value);
		}

	private:
		friend class Job;
		MapTaskRunner() :
			m_dataBuffer(nullptr)
		{
		}

	private:
		BufferPolicy* m_dataBuffer;
	};

	class ReduceTaskRunner
	{
	public:
		inline void Emit(typename ReducePolicy::KeyType key, typename ReducePolicy::ValueType value)
		{
			m_dataDrain->AddData(key, value);
		}

	private:
		friend class Job;
		ReduceTaskRunner() :
			m_dataDrain(nullptr)
		{
		}

	private:
		DrainPolicy* m_dataDrain;
	};

	Job(SourcePolicy& source, DrainPolicy& drain) :
		m_dataBuffer(new BufferPolicy()),
		m_mapRunner(new MapTaskRunner()),
		m_reduceRunner(new ReduceTaskRunner()),
		m_source(source),
		m_drain(drain)
	{
		// Set up the runners
		m_mapRunner->m_dataBuffer = m_dataBuffer;
		m_reduceRunner->m_dataDrain = &m_drain;
	}

	~Job()
	{
		SAFE_FREE(m_mapRunner);
		SAFE_FREE(m_reduceRunner);
		SAFE_FREE(m_dataBuffer);
	}

	// Runs the full map reduce
	void Run()
	{
		// Map it
		MapPolicy mapClass;
		typename MapPolicy::KeyType sourceKey;
		typename MapPolicy::ValueType sourceValue;
		while (m_source.GetData(sourceKey, sourceValue)) 
		{
			mapClass.Map(*m_mapRunner, sourceKey, sourceValue);
		}

		// Reduce it
		ReducePolicy reduceClass;
		typename MapPolicy::IntermediateKeyType intermitKey;
		typename ReduceIteratorType iterStart;
		typename ReduceIteratorType iterEnd;

		while (m_dataBuffer->GetData(intermitKey, iterStart, iterEnd))
		{
			reduceClass.Reduce(*m_reduceRunner, intermitKey, iterStart, iterEnd);
		}
	}

private:
	MapTaskRunner* m_mapRunner;
	ReduceTaskRunner* m_reduceRunner;
	BufferPolicy* m_dataBuffer;
	SourcePolicy& m_source;
	DrainPolicy& m_drain;

	template<typename J>
	friend void _MapThread(J* job);

	template<typename J>
	friend void _ReduceThread(J* job);
};

} // namespace MapReduce
} // namespace Thilenius