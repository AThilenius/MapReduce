#include "stdafx.h"

#include <iostream>
#include <unordered_map>
#include <string>

#include "Job.h"

namespace Thilenius {

class MapTask;
class ReduceTask;

typedef
	MapReduce::Job<
	MapTask,
	ReduceTask>
TokenizeFilesJob;


class MapTask
{
public:
	typedef int KeyType;
	typedef int ValueType;
	typedef int IntermediateKeyType;
	typedef int IntermediateValueType;

	void Map(TokenizeFilesJob::MapTaskRunner& runner, MapTask::KeyType& key, MapTask::ValueType& value)
	{
		runner.Emit(key, value);
	}
};

class ReduceTask
{
public:
	typedef int  KeyType;
	typedef int  ValueType;

	void Reduce(TokenizeFilesJob::ReduceTaskRunner& runner, ReduceTask::KeyType& key, ReduceTask::ValueType& value)
	{
		runner.Emit(key, value);
	}
};

} // namespace Thilenius

void main()
{
	std::unordered_map<int, int> sourceMap;
	std::unordered_map<int, int> outputMap;
	Thilenius::MapReduce::DataSource::StdMapSource<Thilenius::MapTask> source (&sourceMap);
	Thilenius::MapReduce::DataDrain::StdMapDrain<Thilenius::ReduceTask> drain (&outputMap);

	Thilenius::TokenizeFilesJob job;
	job.Run(source, drain);

	std::cin.ignore();
}