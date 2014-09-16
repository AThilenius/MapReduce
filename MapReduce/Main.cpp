#include "stdafx.h"

#include <iostream>
#include <unordered_map>
#include <string>
#include <sstream>

#include "Job.h"
#include "JobScheduler.h"
#include "ThreadedJobScheduler.h"

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
	typedef std::string KeyType;
	typedef int ValueType;
	typedef std::string IntermediateKeyType;
	typedef int IntermediateValueType;

	void Map(
		TokenizeFilesJob::MapTaskRunner& runner, 
		MapTask::KeyType& key, 
		MapTask::ValueType& value)
	{
		std::istringstream stream (key);
		std::string subString;    
		while (std::getline(stream, subString, ' '))
			runner.Emit(subString, 1);
	}
};

class ReduceTask
{
public:
	typedef std::string  KeyType;
	typedef int  ValueType;

	void Reduce(
		TokenizeFilesJob::ReduceTaskRunner& runner, 
		ReduceTask::KeyType& key, 
		TokenizeFilesJob::ReduceIteratorType& begin, 
		TokenizeFilesJob::ReduceIteratorType& end)
	{
		int count = 0;
		for(; begin != end; begin++)
			count++;

		runner.Emit(key, count);
	}
};

} // namespace Thilenius

void main()
{
	std::unordered_map<std::string, int> sourceMap;
	std::unordered_map<std::string, int> outputMap;
	Thilenius::MapReduce::DataSource::StdMapSource<Thilenius::MapTask> source (&sourceMap);
	Thilenius::MapReduce::DataDrain::StdMapDrain<Thilenius::ReduceTask> drain (&outputMap);

	// Fill the Source with crap
	sourceMap.insert(std::pair<std::string, int>("Hello from me", 1));
	sourceMap.insert(std::pair<std::string, int>("another message from me", 1));
	sourceMap.insert(std::pair<std::string, int>("me should show up 3 times", 1));

	Thilenius::TokenizeFilesJob job(source, drain);
	//job.Run();

	Thilenius::MapReduce::ThreadedJobScheduler s;
	s.RunJob<Thilenius::TokenizeFilesJob>(job);

	std::cin.ignore();
}