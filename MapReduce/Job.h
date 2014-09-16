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
	typename MapPolicy,
	typename ReducePolicy,
	typename SourcePolicy = DataSource::StdMapSource<MapPolicy>,
	typename DrainPolicy = DataDrain::StdMapDrain<ReducePolicy>,
	typename BufferPolicy = DataBuffer::StdMapVectorBuffer<MapPolicy>
>
class Job
{
public:
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

	Job() :
		m_dataBuffer(new BufferPolicy()),
		m_mapRunner(new MapTaskRunner()),
		m_reduceRunner(new ReduceTaskRunner())
	{
	}

	~Job()
	{
		SAFE_FREE(m_mapRunner);
		SAFE_FREE(m_reduceRunner);
		SAFE_FREE(m_dataBuffer);
	}

	// Runs the full map reduce
	void Run(SourcePolicy& source, DrainPolicy& drain)
	{
		// Set up the runners
		m_mapRunner->m_dataBuffer = m_dataBuffer;
		m_reduceRunner->m_dataDrain = &drain;

		// Map it
		MapPolicy mapClass;
		typename MapPolicy::KeyType sourceKey;
		typename MapPolicy::ValueType sourceValue;
		while (source.GetData(sourceKey, sourceValue)) 
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

	MapTaskRunner* m_mapRunner;
	ReduceTaskRunner* m_reduceRunner;
	BufferPolicy* m_dataBuffer;
};

} // namespace MapReduce
} // namespace Thilenius