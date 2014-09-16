#include "stdafx.h"

#include <iostream>
#include <map>
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

	bool Map(TokenizeFilesJob::MapTaskRunner& runner, MapTask::KeyType& key, MapTask::ValueType& value)
	{

	}
};

class ReduceTask
{
public:
	typedef int  KeyType;
	typedef int  ValueType;

	bool Reduce(TokenizeFilesJob::ReduceTaskRunner& runner, ReduceTask::KeyType& key, ReduceTask::ValueType& value)
	{

	}
};

} // namespace Thilenius

void main()
{
	std::map<std::string, int> sourceMap;
	std::map<std::string, int> outputMap;

	Thilenius::TokenizeFilesJob job;

	std::cin.ignore();
}