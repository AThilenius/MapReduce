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
	typename MapPolicy,
	typename ReducePolicy,
	typename SourcePolicy = DataSource::FileLoaderSource<MapPolicy>,
	typename DrainPolicy = DataDrain::StdMapDrain<ReducePolicy>,
	typename BufferPolicy = DataBuffer::StdMapVectorBuffer<MapPolicy>
>
class Job
{
public:
	typedef MapPolicy MapPolicyType;
	typedef ReducePolicy ReducePolicyType;
	typedef typename SourcePolicy::InputType SourceInputType;
	typedef typename DrainPolicy::InputType DrainInputType;
	typedef typename BufferPolicy::ValueVectorIteratorType ReduceIteratorType;

	class MapTaskRunner
	{
	public:
		inline void Emit(typename MapPolicy::IntermediateKeyType key, typename MapPolicy::IntermediateValueType value)
		{
			LocalBuffer.AddData(key, value);
		}

		BufferPolicy LocalBuffer;
	};

	class ReduceTaskRunner
	{
	public:
		inline void Emit(typename ReducePolicy::KeyType key, typename ReducePolicy::ValueType value)
		{
			LocalBuffer.AddData(key, value);
		}

		DrainPolicy LocalBuffer;

	private:
		friend class Job;
	};

	Job(SourceInputType& source, DrainInputType& drain) :
		DataBuffer(new BufferPolicy()),
		Source(new SourcePolicy(&source)),
		Drain(new DrainPolicy(&drain))
	{
	}

	~Job()
	{
		SAFE_FREE(DataBuffer);
		SAFE_FREE(Source);
		SAFE_FREE(Drain);
	}

	void Shuffle()
	{
		DataBuffer->Shuffle();
	}

public:
	// Owned
	BufferPolicy* DataBuffer;
	SourcePolicy* Source;
	DrainPolicy* Drain;

};

} // namespace MapReduce
} // namespace Thilenius