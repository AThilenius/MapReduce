#include "stdafx.h"

#include <iostream>
#include <unordered_map>
#include <string>
#include <fstream>
#include <sstream>

#include "Job.h"
#include "JobScheduler.h"
#include "ThreadedJobScheduler.h"
#include "SelectFirstReducer.h"

namespace Thilenius {
namespace MapReduce {

class MapTask;
class ReduceTask;

typedef
MapReduce::Job<
	MapTask,
	Reducers::SelectFirstReducer<MapTask>
>
TokenizeFilesJob;


class MapTask
{
public:
	typedef std::string		KeyType;
	typedef std::ifstream*	ValueType;
	typedef std::string		IntermediateKeyType;
	typedef std::string		IntermediateValueType;

	template<
		typename Runner
	>
	void Map(Runner& runner, KeyType& key, ValueType& value)
	{
		std::string word;
		while(*value >> word)
			runner.Emit(word, "");
	}
};

//class ReduceTask
//{
//public:
//	typedef std::string		KeyType;
//	typedef std::string		ValueType;
//
//	template<
//		typename Runner,
//		typename ItorType
//	>
//	void Reduce(Runner& runner, KeyType& key, ItorType& begin, ItorType& end)
//	{
//		for(; begin != end; begin++)
//			runner.Emit(key, *begin);
//	}
//};

} // namespace MapReduce
} // namespace Thilenius

void main()
{

	std::ifstream* stream = new std::ifstream("C:\\Users\\Alec\\Desktop\\names1.txt");
	std::string word;
	while(*stream >> word)
		std::cout << word << std::endl;

	std::string inputTokens ("C:\\Users\\Alec\\Desktop\\names1.txt");
	std::unordered_map<std::string, std::string> outputMap;

	Thilenius::MapReduce::TokenizeFilesJob job(inputTokens, outputMap);

	Thilenius::MapReduce::JobScheduler scheduler;
	scheduler.RunJob<Thilenius::MapReduce::TokenizeFilesJob>(job);

	std::cin.ignore();
}