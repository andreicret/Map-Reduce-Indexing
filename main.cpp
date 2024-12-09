#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <unordered_map>
#include <vector>
#include <queue>
#include <set>
#include "pthread.h"

using namespace std;

#define ALPHABET_SIZE 26
#define DIE(condition, message, ...) \
	do { \
		if ((condition)) { \
			fprintf(stderr, "[(%s:%d)]: " # message "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
			perror(""); \
			exit(1); \
		} \
	} while (0)

/**
 * @brief Comporator used in my final list of words. Each letter has a
 * corresponding priority queue that stores words based on these criteria
 */
struct Compare {
    bool operator()(const pair<string, set<uint32_t>>& a,
		const pair<string, set<uint32_t>>& b) const {
       
        if (a.second.size() != b.second.size()) {
            return a.second.size() < b.second.size(); 
        }
        
        return a.first > b.first; 
    }
};

/**
 * @brief Data used by Mapper Threads
 * 
 */
struct MapperArg {
	pthread_mutex_t *mutex;
	pthread_barrier_t *barrier;
	queue<pair<string, uint32_t>> *files;
	unordered_map<string, set<uint32_t>> *wordFrequency;
};

/**
 * @brief Data used by Reducer Threads
 * 
 */
struct ReducerArg {
	uint32_t threadID;
	uint32_t reducerNo;
	pthread_mutex_t *mutex;
	pthread_barrier_t *barrier;
	pthread_barrier_t *innerBarrier;
	unordered_map<string, set<uint32_t>> *wordFrequency;
	vector<priority_queue<pair<string, set<uint32_t>>, 
            vector<pair<string, set<uint32_t>>>, Compare>> *letterList;
	queue<string>*outFiles;
	
	// Struct constructor
	ReducerArg(uint32_t id, uint32_t no, pthread_mutex_t* m,
			pthread_barrier_t* b, pthread_barrier_t* iB,
            unordered_map<string, set<uint32_t>> * freq)
        : threadID(id), reducerNo(no), mutex(m), barrier(b), innerBarrier(iB),
		 wordFrequency(freq) {}
	
};

/**
 * @brief Takes a string and parses it as an lowercase, punctuation-free word
 * 
 * @param str The initial string that will be parsed
 */
void parse_word(string& str) {
    string result;
    for (char c : str) {
        if (isalpha(c)) {
            result += tolower(c);
        }
    }
	/*Put the result back*/ 
    str.swap(result);
}

/**
 * @brief Mapper functionality. Each thread takes a file from the file queue,
 * parses the words and adds in the word hashmap the files in which it is 
 * present. A temporary hashmap is used to avoid bottlenecks.
 * 
 * @param arg An object of type MapperArg*
 * @return void* Everything went well.
 */
void* mapper_func(void* arg) {
    MapperArg* mapperData = (MapperArg*)arg;
    string currentFile;
    uint32_t fileIndex;
	
	/*Temporary variable to avoid mutex bottleneck*/
	unordered_map<string, set<uint32_t>> localWordFrequency;
	
	/*Take a fileName from queue*/
    while (true) {
        /* Critical section */
		 DIE(pthread_mutex_lock(mapperData->mutex), "Mutex lock");
			if (mapperData->files->empty()) {
				DIE(pthread_mutex_unlock(mapperData->mutex), "Mutex unlock");
				break;
			}
			currentFile = mapperData->files->front().first;
			fileIndex = mapperData->files->front().second;
			mapperData->files->pop();

		DIE(pthread_mutex_unlock(mapperData->mutex), "Mutex unlock");
		
        /* Process the file */
        ifstream in(currentFile);
        if (!in.is_open()) {

            cerr << "Failed to open file: " << currentFile << endl;
            continue;
        }

    	string word;

		/*Add the file indices*/
		while (in >> word) {
			parse_word(word);
			auto& vec = localWordFrequency[string(word)];

			for (auto it : vec) {
				if (it == fileIndex)
					continue;
			}
			vec.insert(fileIndex);
		}
		in.close();
    }

	/*Make the frequency updates persistent*/
	for (const auto& entry : localWordFrequency) {
		DIE(pthread_mutex_lock(mapperData->mutex), "Mutex lock");
		auto& globalSet = (*mapperData->wordFrequency)[entry.first];
		globalSet.insert(entry.second.begin(), entry.second.end());
		DIE(pthread_mutex_unlock(mapperData->mutex), "Mutex unlock");
	}

	/*Reducers shall not start before all mappers are done parsing files*/
	pthread_barrier_wait(mapperData->barrier);
    return NULL;
}

/**
 * @brief Reducer functionality. Each pair <word, files> are inserted in each
 * letter's priority queue. The workload is separated in chunks for a better
 * parallellization. Each thread will then write the results in a specific file
 * 
 * @param arg ReducerArg* variable
 * @return void* Everything went well.
 */
void* reducer_func(void* arg) {
	ReducerArg* reducerData = (ReducerArg*) arg;
	unordered_map<string, set<uint32_t>>&wordFrequency =*
		(reducerData->wordFrequency);

	/*Make sure all mappers are done and all reducers reach this point*/
	pthread_barrier_wait(reducerData->barrier);

	/*Separate no. of words in chunks*/
	size_t chunkSize = wordFrequency.size() / reducerData->reducerNo;

	/*Iterator used to reach the specified chunk*/
    auto it = wordFrequency.begin();
    advance(it, reducerData->threadID * chunkSize);
    auto endIt = (reducerData->threadID == reducerData->reducerNo - 1) 
                 ? wordFrequency.end() 
                 : next(it, chunkSize);

	/* For each chunk of data, put them in letter's priority queue*/
	DIE(pthread_mutex_lock(reducerData->mutex), "Mutex lock");
	for (auto i = it; i != endIt; i++ ) {
		char let = i->first[0];
		size_t index = let - 'a';

		if (!(index >= 0 && index <= 25)) {
			continue;
		}

       	(*reducerData->letterList)[index].push({i->first, i->second}); 
	}
	DIE(pthread_mutex_unlock(reducerData->mutex), "Mutex unlock");

	/*Letter queue, print the results*/
	pthread_barrier_wait(reducerData->innerBarrier);

	 while (true) {
        DIE(pthread_mutex_lock(reducerData->mutex), "Mutex lock");
		if (reducerData->outFiles->empty()) {
			DIE(pthread_mutex_unlock(reducerData->mutex), "Mutex unlock");
			break;
		}

		string fileName = reducerData->outFiles->front();
		reducerData->outFiles->pop();
       DIE(pthread_mutex_unlock(reducerData->mutex), "Mutex unlock");

		ofstream out(fileName);
        auto& pq =  (*reducerData->letterList)[fileName[0] - 'a'];
		
		if (fileName[0] - 'a' < 0 || fileName[0] - 'a' > 25)
			continue;
		
		/*Empty each letter's priority queue*/
        while (!pq.empty()) {
			
            auto entry = pq.top(); 
            pq.pop();
            out << entry.first << ":[";
			bool first = true; 

			for (auto it = entry.second.begin(); it != entry.second.end(); ++it) {
				if (!first) {
					out << " "; 
				}
				out << *it;
				first = false; 
			}
            out << "]\n";
        }
        out.close(); 
	 }
	return NULL;
}

/**
 * @brief Main function. Init threads, mutex and barriers. Takes input from
 * stdin
 * 
 * @param argc stdin argument count
 * @param argv stdin arguments
 * @return int Program status code
 */
int main(int argc, char **argv) {
	/*Declarations*/
    size_t mapperNo, reducerNo, fileNo;
    string inputFile;
    vector<pthread_t> threads;
    queue<pair<string, uint32_t>> fileQueue;
    pthread_mutex_t mutex;
	pthread_barrier_t barrier;
	pthread_barrier_t reducerBarrier;
	unordered_map<string, set<uint32_t>> wordFrequency;
	vector<priority_queue<pair<string, set<uint32_t>>, 
                                   vector<pair<string, set<uint32_t>>>, 
                                   Compare>> letterList(ALPHABET_SIZE);
	void* status;
	
	/*Check for parameter correctness*/
    DIE(argc != 4, "Invalid number of parameters");
   
    mapperNo = stoi(argv[1]);
    reducerNo = stoi(argv[2]);
    inputFile = argv[3];

    /* Parse input file */
    ifstream input(inputFile);
	DIE(!input, "File open");
	
    string line;
    input >> line;
    fileNo = stoi(line);
	
	/*Add each file name in the file queue for mapper threads*/
    for (size_t i = 0; i < fileNo; i++) {
        input >> line;
        fileQueue.push(make_pair(line, i + 1));
    }

	/*Initialize barriers and mutexes*/
	DIE(pthread_mutex_init(&mutex, NULL), "Error at mutex creation");
	DIE(pthread_barrier_init(&barrier, NULL, mapperNo + reducerNo), "Barrier init");
	DIE(pthread_barrier_init(&reducerBarrier, NULL, reducerNo), "Barrier init");

	/*Initalize mapper argument*/
    MapperArg mapperArg = {
        .mutex = &mutex,
		.barrier = &barrier,
        .files = &fileQueue,
		.wordFrequency = &wordFrequency
    };

	/*Generate a file queue for each letter*/
	queue<string>outFiles;
	for (size_t i = 0; i < ALPHABET_SIZE; i++) {
		string fileName = string(1, i + 'a') + ".txt";
		outFiles.push(fileName);
	}

	/*Initialize reducer args*/
	vector<ReducerArg> reducerArgs;
	for (size_t i = 0; i < reducerNo; i++) {
		reducerArgs.emplace_back(i, reducerNo, &mutex, &barrier, &reducerBarrier, &wordFrequency);
		reducerArgs[i].letterList = &letterList;
		reducerArgs[i].outFiles = &outFiles;
	}

    /* Start threads */
    pthread_t thread;
    for (size_t i = 0; i < mapperNo + reducerNo; i++) {
		if (i < mapperNo) {
			DIE(pthread_create(&thread, NULL, mapper_func, &mapperArg), "Thread creation");
			threads.push_back(thread);
		} else {
			DIE(pthread_create(&thread, NULL, reducer_func, &(reducerArgs[i - mapperNo])), "Thread creation");
			threads.push_back(thread);
		}
    }

    /* Join threads */
    for (pthread_t thread : threads) {
        DIE(pthread_join(thread, &status), "Thread join");
    }

	/*Destroy mutex and barriers*/
    DIE(pthread_mutex_destroy(&mutex), "Mutex destruction");
	DIE(pthread_barrier_destroy(&barrier), "Barrier destruction");
	DIE(pthread_barrier_destroy(&reducerBarrier), "Barrier destruction");

    return 0;
}
