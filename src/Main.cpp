#include "stdafx.h"

#include <iostream>
#include <unordered_map>
#include <string>
#include <fstream>
#include <sstream>
#include <functional>
#include <ctime>
#include <streambuf>

#include "Job.h"
#include "JobScheduler.h"
#include "ThreadedJobScheduler.h"
#include "StdMapSource.h"

namespace Thilenius {
namespace MapReduce {

class MapTask;
class ReduceTask;

typedef
MapReduce::Job<
	MapTask,
	ReduceTask,
	DataSource::StdMapSource<MapTask>
	//Reducers::SelectFirstReducer<MapTask>
>
TokenizeFilesJob;


class MapTask
{
public:
	typedef std::string		KeyType;
	typedef std::string		ValueType;
	typedef std::string		IntermediateKeyType;
	typedef int				IntermediateValueType;

	template<
		typename Runner
	>
	void Map(Runner& runner, KeyType& key, ValueType& value)
	{
		int bufferIndex = 0;
		char* wordBuffer = new char[256];

		for (int i = 0; i < value.size(); i++)
		{
			char thisChar = value[i];

			if (thisChar == '\n')
			{
				wordBuffer[bufferIndex] = NULL;
				runner.Emit(std::string(wordBuffer), 1);
				bufferIndex = 0;
				continue;
			}

			wordBuffer[bufferIndex++] = thisChar;
		}
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

int main()
{
	//test();

#ifdef __APPLE__
	std::string inputTokens ("/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile1.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile2.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile3.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile4.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile5.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile6.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile7.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile8.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile9.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile10.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile11.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile12.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile13.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile14.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile15.txt "
                             "/Volumes/BOOTCAMP/Users/Alec/Documents/Development/CSharp/LargeTextGenerator/bin/Debug/LargeFile16.txt");
#endif
    
#ifdef _WIN32
	std::string inputTokens ("C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile1.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile2.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile3.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile4.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile5.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile6.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile7.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile8.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile9.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile10.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile11.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile12.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile13.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile14.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile15.txt "
							 "C:\\Users\\Alec\\Documents\\Development\\CSharp\\LargeTextGenerator\\bin\\Debug\\LargeFile16.txt");
#endif

	std::unordered_map<std::string, std::string> cachedFiles;
	std::unordered_map<std::string, std::string> cachedFiles2;

	// Cache all file data in-memory
	std::cout << "Pre Caching..." << std::endl;
	std::string filePath;
	std::stringstream ss(inputTokens);
	while (ss >> filePath)
	{
		std::ifstream fileStream (filePath);
		std::string str;

		// Pre-alloc memory
		fileStream.seekg(0, std::ios::end);
		str.reserve(fileStream.tellg());
		fileStream.seekg(0, std::ios::beg);

		str.assign((std::istreambuf_iterator<char>(fileStream)),
					std::istreambuf_iterator<char>());

		cachedFiles.insert(std::pair<std::string, std::string>(filePath, str));
		cachedFiles2.insert(std::pair<std::string, std::string>(filePath, str));
	}

	std::cout << "Done. Press any key to run analysis" << std::endl;
	std::cin.ignore();

	//std::cout << "Running single threaded" << std::endl;
	//{
	//	std::clock_t startSingle = std::clock();

	//	std::unordered_map<std::string, int> outputMap;
	//	Thilenius::MapReduce::TokenizeFilesJob job(cachedFiles, outputMap);
	//	Thilenius::MapReduce::JobScheduler scheduler;
	//	scheduler.RunJob(job);

	//	std::cout << "MapReduced " << outputMap.size() << " unique values." << std::endl;
	//	std::cout << "Singe-threaded took: "<< (( std::clock() - startSingle ) / (double) CLOCKS_PER_SEC ) << " seconds" << std::endl;
	//}

	std::cout << "Running multi threaded" << std::endl;
	{
		std::clock_t startMulti = std::clock();

		std::unordered_map<std::string, int> outputMap;
		Thilenius::MapReduce::TokenizeFilesJob job(cachedFiles2, outputMap);
		Thilenius::MapReduce::ThreadedJobScheduler scheduler;
		scheduler.RunJob(job);

		std::cout << "MapReduced " << outputMap.size() << " unique values." << std::endl;
		std::cout << "Mutli-threaded took: "<< (( std::clock() - startMulti ) / (double) CLOCKS_PER_SEC ) << " seconds" << std::endl;
	}

	std::cin.ignore();
}