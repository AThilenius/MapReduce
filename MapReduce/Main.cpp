#include "stdafx.h"

#include <iostream>
#include <unordered_map>
#include <string>
#include <fstream>
#include <sstream>
#include <functional>

#include "Job.h"
#include "JobScheduler.h"
#include "ThreadedJobScheduler.h"
#include "SelectFirstReducer.h"
#include "LockstepTaskRunner.h"

namespace Thilenius {
namespace MapReduce {

class MapTask;
class ReduceTask;

typedef
MapReduce::Job<
	MapTask,
	ReduceTask
	//Reducers::SelectFirstReducer<MapTask>
>
TokenizeFilesJob;


class MapTask
{
public:
	typedef std::string		KeyType;
	typedef std::ifstream*	ValueType;
	typedef std::string		IntermediateKeyType;
	typedef int				IntermediateValueType;

	template<
		typename Runner
	>
	void Map(Runner& runner, KeyType& key, ValueType& value)
	{
		std::string word;
		while(*value >> word)
			runner.Emit(word, 0);
	}
};

class ReduceTask
{
public:
	typedef std::string		KeyType;
	typedef int				ValueType;

	template<
		typename Runner,
		typename ItorType
	>
	void Reduce(Runner& runner, KeyType& key, ItorType& begin, ItorType& end)
	{
		int total = 0;

		for(; begin != end; begin++)
			total++;

		runner.Emit(key, total);
	}
};

} // namespace MapReduce
} // namespace Thilenius

void _OneArgFunction(int a1){}

void main()
{
	std::string inputTokens (
		"C:\\Users\\Alec\\Documents\\Development\\CPP\\MapReduce\\LargeTextGenerator\\bin\\Debug\\LargeFile1.txt "
		"C:\\Users\\Alec\\Documents\\Development\\CPP\\MapReduce\\LargeTextGenerator\\bin\\Debug\\LargeFile2.txt "
		"C:\\Users\\Alec\\Documents\\Development\\CPP\\MapReduce\\LargeTextGenerator\\bin\\Debug\\LargeFile3.txt "
		"C:\\Users\\Alec\\Documents\\Development\\CPP\\MapReduce\\LargeTextGenerator\\bin\\Debug\\LargeFile4.txt "
		"C:\\Users\\Alec\\Documents\\Development\\CPP\\MapReduce\\LargeTextGenerator\\bin\\Debug\\LargeFile5.txt "
		"C:\\Users\\Alec\\Documents\\Development\\CPP\\MapReduce\\LargeTextGenerator\\bin\\Debug\\LargeFile6.txt "
		"C:\\Users\\Alec\\Documents\\Development\\CPP\\MapReduce\\LargeTextGenerator\\bin\\Debug\\LargeFile7.txt "
		"C:\\Users\\Alec\\Documents\\Development\\CPP\\MapReduce\\LargeTextGenerator\\bin\\Debug\\LargeFile8.txt");
	std::unordered_map<std::string, int> outputMap;

	Thilenius::MapReduce::TokenizeFilesJob job(inputTokens, outputMap);

	Thilenius::MapReduce::ThreadedJobScheduler scheduler;
	scheduler.RunJob(job);

	std::cout << "MapReduced " << outputMap.size() << " unique values." << std::endl;
	//std::cin.ignore();
}