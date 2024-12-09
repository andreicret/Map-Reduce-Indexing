#####  Copyright 2024 Alexandru-Andrei CRET (alexandru.cret04@gmail.com)

# Parallel Computation of an Inverted Index Using the Map-Reduce Paradigm

This project is intended to implement the Map-Reduce Paradigm. Its purpose is to offer scalability for solving reading, parsing and sorting a large amount of data in a parallelized manner. The code is written in C++ programming language.

## Data Structures
+ `MapperArg`: a struct used for sending data to Mapper threads. It includes a mutex, a barrier, a queue of file names to be processed and an unordered_map, `wordFrequency`

+ `ReducerArg`: a struct used for sending data to Reducer threads. In order to separate data into chunks, the ID of each reducer was needed. There are also a mutex and two barriers, wordFrequency and a vector of priority queues, one for each letter in the alphabet

+ `wordFrequency` is a hashmap whose keys are the words that have been read and values a set of file indices, indicating in which files that word is present. Declared(as well as other variables) in `main`, its pointer is sent to each thread via the structures mentioned above

+ `letterList` is a vector of priority queues that stores the pairs <word, list_of_file_indices> in a sorted way. The vector represents the alphabet (e.g. letterList[1] is the priority queue which corresponds to letter 'b'). A custom comparator was created so that the words are sorted by two criteria: the no. of file indices and lexicographically:

```C++
struct Compare {
    bool operator()(const pair<string, set<uint32_t>>& a,
		const pair<string, set<uint32_t>>& b) const {
        /* First criteria */
        if (a.second.size() != b.second.size()) {
            return a.second.size() < b.second.size(); 
        }
        /* Second criteria */
        return a.first > b.first; 
    }
};
```
+ `fileQueue` and `outFiles` are two queues used for storing file names and output file names (one for each letter of the alphabet) they facilitate a dynamic and fast approach for threads.

## Parallelization
The use of multiple threads can offer a significant scalability for proccesing an immense amount of data. A straight-forward manner for a multithreading program is using the C/C++ library `pthread.h`. The number of mappers and reducers is established at the start of the program, from stdin,  and a number of `mapperNo + reducerNo`
threads are started. 
## Mapper 

Each mapper thread processes files from the file queue dynamically so that idle states are avoided. Extracting a file from the queue implied a critical section fenced by a mutex. `parse_word` function converts each string that was read into a lowercase word (only alphabetic characters). In order to avoid bottlenecks caused by abusing critical sections, `localWordFrequency` was declared for adding file indices for each word so that the "global" hashmap is updated after all mappers have finished processing.
## Reducer

With a valid word hashmap, the reducers' job is to filter them for each letter. Each reducer (with its own ID) has to process a specific chunk of `wordFrequency`, which was made possible with an iterator:

```C++
	/*Iterator used to reach the specified chunk*/
    auto it = wordFrequency.begin();
    advance(it, reducerData->threadID * chunkSize);
    auto endIt = (reducerData->threadID == reducerData->reducerNo - 1) 
                 ? wordFrequency.end() 
                 : next(it, chunkSize);
    /* other operations... */
```
Each pair <word, list_of_indices> is inserted in each letter's priority queue in a sorted manner.

For each priority queue, the data is printed in each letter's specific file in a parallel way, similar to how files were initially processed. 
## Synchronization
No sooner the mappers have done processing files the reducers can start their own processing. Therefore, a  barrier which requires `mapperNo + reducerNo` threads is used at the end of each mapper and at the start of each reducer. Before letter-files are populated, another barrier makes sure that all reducers have done inserting data in the `letterList`.

Mutexes were used in critical sections so that no other thread could alter a shared memory section by accessing it simultaneously.

## Program flow

Main function declares and initializes every variable, data structure and thread. The processing tasks happen once the mappers and reducers are started.
The `DIE` macro is useful in order to enforce a defensive approach of the code. `valgrind` was used for memory safety.