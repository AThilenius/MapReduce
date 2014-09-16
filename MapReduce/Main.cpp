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

	template<
		typename Runner
	>
	void Map(Runner& runner, KeyType& key, ValueType& value)
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

	template<
		typename Runner,
		typename ItorType
	>
	void Reduce(Runner& runner, KeyType& key, ItorType& begin, ItorType& end)
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

	// Fill the Source with crap
	sourceMap.insert(std::pair<std::string, int>("Hello from me", 1));
	sourceMap.insert(std::pair<std::string, int>("another message from me", 1));
	sourceMap.insert(std::pair<std::string, int>("me should show up 3 times", 1));

	Thilenius::TokenizeFilesJob job(sourceMap, outputMap);

	Thilenius::MapReduce::ThreadedJobScheduler s;
	s.RunJob<Thilenius::TokenizeFilesJob>(job);

	std::cin.ignore();
}