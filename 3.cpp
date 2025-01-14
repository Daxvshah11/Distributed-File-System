#include <mpi.h>
#include <iostream>
#include <vector>
#include <string>
#include <functional>
#include <map>
#include <unistd.h>
#include <set>
#include <thread>
#include <chrono>
#include <mutex>
#include <fstream>
#include <atomic>
#include <sstream>
#include <cstring>
#include <iomanip>
#include <algorithm>
#include <unordered_set>

using namespace std;

// constants
const int CHUNK_SIZE = 32;
const int REPLICATION_FACTOR = 3;
const int HEARTBEAT_INTERVAL = 2;
const int FAILURE_DETECTION_TIMEOUT = 5;

// MESSAGE TAGS
enum MessageType
{
    UPLOAD_REQUEST = 1,
    CHUNK_DATA = 2,
    DOWNLOAD_REQUEST = 3,
    HEARTBEAT = 5,
    EXIT_TAG = 999
};

// STRUCT for FILE METADATA
struct FileMetadata
{
    string fileName;
    string fileAddress;
    int numChunks;
    long long int startChunkID;
    map<long long int, unordered_set<int>> chunkPositions; // chunkID : replicaNodes
};

// FUNCTIONS DECLARATIONs
void processMetadataServer(int numProcesses);
void processStorageNode(int myRank);

pair<int, string> opUploadMS(long long int &lastChunkID, string fileName, string fileAddress, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes);
pair<int, string> opRetrieveMS(string fileName, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes);
void printFileData(string fileData);
pair<int, string> opSearchMS(string fileName, string word, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes);
pair<int, string> opListFileMS(string fileName, map<string, FileMetadata> &metadata);

void opUploadSS(map<int, string> &storedChunks);
void opRetrieveSS(map<int, string> &storedChunks);

pair<int, string> readFileInChunks(const string &fileName, const string &fileAddress, vector<string> &fileChunks);

/*































*/

// processing metadata server
void processMetadataServer(int numProcesses)
{
    // Metadata Storage
    map<string, FileMetadata> metadata; // (fileName, FileMetadata)
    unordered_set<int> activeNodes;
    long long int lastChunkID = 0;

    // updating active nodes
    for (int i = 1; i < numProcesses; i++)
    {
        activeNodes.insert(i);
    }

    // getting constant input commands
    string command;
    bool isRunning = true;
    while (isRunning)
    {
        // getting command
        getline(cin, command);

        // processing command
        istringstream iss(command);
        string operation;
        iss >> operation;

        // checking which operation
        if (operation == "exit")
        {
            // if anything else : ERROR
            string temp;
            iss >> temp;
            if (!temp.empty())
            {
                cout << -1 << " : Invalid command\n"
                     << endl;
                continue;
            }

            // notifying all Storage Servers to terminate
            for (int i = 1; i < numProcesses; i++)
            {
                MPI_Send(nullptr, 0, MPI_BYTE, i, EXIT_TAG, MPI_COMM_WORLD);
            }

            isRunning = false;
        }
        else if (operation == "upload")
        {
            // getting file name
            string fileName;
            iss >> fileName;

            string fileAddress;
            iss >> fileAddress;

            string temp;
            iss >> temp;

            // checking correct inputs
            if (fileName.empty() || fileAddress.empty() || !temp.empty())
            {
                cout << -1 << " : Invalid command\n"
                     << endl;
                continue;
            }

            // handling the op
            pair<int, string> retVal = opUploadMS(lastChunkID, fileName, fileAddress, metadata, activeNodes);
            cout << retVal.first << " : " << retVal.second << endl;
        }
        else if (operation == "retrieve")
        {
            string fileName;
            iss >> fileName;

            string temp;
            iss >> temp;

            // checking correct inputs
            if (fileName.empty() || !temp.empty())
            {
                cout << -1 << " : Invalid command\n"
                     << endl;
                continue;
            }

            // handling the op
            pair<int, string> retVal = opRetrieveMS(fileName, metadata, activeNodes);
            if (retVal.first == -1)
                cout << retVal.first << " : " << retVal.second << endl;
            else
            {
                printFileData(retVal.second);
                cout << 1 << " : File retrieved successfully\n"
                     << endl;
            }
        }
        else if (operation == "search")
        {
            // getting file name
            string fileName;
            iss >> fileName;

            string word;
            iss >> word;

            string temp;
            iss >> temp;

            // checking correct inputs
            if (fileName.empty() || word.empty() || !temp.empty())
            {
                cout << -1 << " : Invalid command\n"
                     << endl;
                continue;
            }

            // handling the op
            pair<int, string> retVal = opSearchMS(fileName, word, metadata, activeNodes);
            cout << retVal.first << " : " << retVal.second << endl;
        }
        else if (operation == "list_file")
        {
            // getting file name
            string fileName;
            iss >> fileName;

            string temp;
            iss >> temp;

            // if no fileName is entered then error
            if (fileName.empty() || !temp.empty())
            {
                cout << -1 << " : Invalid command\n"
                     << endl;
                continue;
            }

            // handling the op
            pair<int, string> retVal = opListFileMS(fileName, metadata);
            cout << retVal.first << " : " << retVal.second << endl;
        }
        else
        {
            cout << -1 << " : Invalid command\n"
                 << endl;
        }
    }

    return;
}

// processing storage node
void processStorageNode(int myRank)
{
    // getting PID of the process
    cout << "Process rank " << myRank << " has PID: " << getpid() << endl;

    // storage for chunks
    map<int, string> storedChunks;

    // looping to receive commands from Metadata Server
    bool isRunning = true;
    while (isRunning)
    {
        // checking for commands
        MPI_Status status;
        int msgPresent;
        MPI_Iprobe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &msgPresent, &status);

        // checking if message is present
        if (msgPresent)
        {
            // checking which command
            if (status.MPI_TAG == EXIT_TAG)
            {
                // receiving the sent message & exiting
                MPI_Recv(nullptr, 0, MPI_BYTE, 0, EXIT_TAG, MPI_COMM_WORLD, &status);
                isRunning = false;
            }
            else if (status.MPI_TAG == UPLOAD_REQUEST)
            {
                // receiving the sent message
                MPI_Recv(nullptr, 0, MPI_BYTE, 0, UPLOAD_REQUEST, MPI_COMM_WORLD, &status);

                // handling the request
                opUploadSS(storedChunks);
            }
            else if (status.MPI_TAG == DOWNLOAD_REQUEST)
            {
                // receiving the sent message
                MPI_Recv(nullptr, 0, MPI_BYTE, 0, DOWNLOAD_REQUEST, MPI_COMM_WORLD, &status);

                // handling the request
                opRetrieveSS(storedChunks);
            }
            else
            {
                // doing something
                continue;
            }
        }
    }
}

/*






























*/

pair<int, string> opUploadMS(long long int &lastChunkID, string fileName, string fileAddress, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes)
{
    // checking if already present
    if (metadata.find(fileName) != metadata.end())
    {
        return {-1, "File already present in the system\n"};
    }

    // adding to metadata
    metadata[fileName].fileName = fileName;
    metadata[fileName].fileAddress = fileAddress;
    metadata[fileName].startChunkID = lastChunkID;

    // chunking it
    vector<string> fileChunks;
    pair<int, string> retVal = readFileInChunks(fileName, fileAddress, fileChunks);
    if (retVal.first == -1)
        return retVal;

    // updating metadata
    metadata[fileName].numChunks = fileChunks.size();

    // splitting chunks across ACTIVE servers, RR
    auto nodeIterator = activeNodes.begin();
    for (int i = 0; i < fileChunks.size(); i++)
    {
        // getting chunk
        string chunk = fileChunks[i];

        // getting replica nodes
        unordered_set<int> replicaNodes;
        int totalReplicaNodes = min(REPLICATION_FACTOR, static_cast<int>(activeNodes.size()));
        auto it2 = nodeIterator;
        for (int j = 0; j < totalReplicaNodes; j++)
        {
            replicaNodes.insert(*it2);
            it2++;
            if (it2 == activeNodes.end())
                it2 = activeNodes.begin();
        }

        // incrementing node iterator
        nodeIterator++;
        if (nodeIterator == activeNodes.end())
            nodeIterator = activeNodes.begin();

        // updating metadata
        metadata[fileName].chunkPositions[lastChunkID] = replicaNodes;

        // sending chunks to replica nodes
        for (int nodeRank : replicaNodes)
        {
            // letting the node know that there is upload request
            MPI_Send(nullptr, 0, MPI_BYTE, nodeRank, UPLOAD_REQUEST, MPI_COMM_WORLD);

            // sending chunk ID
            MPI_Send(&lastChunkID, 1, MPI_LONG_LONG, nodeRank, UPLOAD_REQUEST, MPI_COMM_WORLD);

            // sending chunk data
            MPI_Send(chunk.c_str(), CHUNK_SIZE, MPI_CHAR, nodeRank, CHUNK_DATA, MPI_COMM_WORLD);
        }

        // incrementing last chunk ID
        lastChunkID++;
    }

    return {1, "File uploaded successfully\n"};
}

void opUploadSS(map<int, string> &storedChunks)
{
    // creating variables
    char buffer[CHUNK_SIZE];
    long long int chunkID;

    // receiving chunk ID
    MPI_Recv(&chunkID, 1, MPI_LONG_LONG, 0, UPLOAD_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // receiving chunk data
    MPI_Recv(buffer, CHUNK_SIZE, MPI_CHAR, 0, CHUNK_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // storing the chunk
    storedChunks[chunkID] = buffer;

    return;
}

pair<int, string> readFileInChunks(const string &fileName, const string &fileAddress, vector<string> &fileChunks)
{
    // opening the file with relative fileAddress
    ifstream file(fileAddress, ios::binary); // binary mode
    if (!file.is_open())
    {
        return {-1, "File not found\n"};
    }

    // reading the file in 32 byte sized chunks
    char buffer[CHUNK_SIZE];
    while (file.read(buffer, sizeof(buffer)))
    {
        // adding full chunk to vector
        fileChunks.emplace_back(buffer, sizeof(buffer));
    }

    // handling the last chunk
    streamsize bytesRead = file.gcount(); // Get the number of bytes read
    if (bytesRead > 0)
    {
        // reading the remaning bytes
        string lastChunk(buffer, bytesRead);

        // padding with null
        lastChunk.append(32 - bytesRead, '\0');

        fileChunks.push_back(lastChunk);
    }

    // closing the file
    file.close();

    return {1, "File read successfully\n"};
}

/*





























*/

void printFileData(string fileData)
{
    // printing file data
    cout << "--------------------------------------------------------------------------- FILE ---------------------------------------------------------------------------" << endl;
    cout << fileData << endl;
    cout << "--------------------------------------------------------------------------- FILE ---------------------------------------------------------------------------" << endl;

    return;
}

pair<int, string> opRetrieveMS(string fileName, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes)
{
    // checking if file is present or not
    if (metadata.find(fileName) == metadata.end())
    {
        return {-1, "File not found\n"};
    }

    // getting metadata
    FileMetadata fileMD = metadata[fileName];

    // iterating over all chunks
    string fileData;
    for (int i = 0; i < fileMD.numChunks; i++)
    {
        // getting chunk ID
        long long int chunkID = fileMD.startChunkID + i;

        // iterating over replica nodes
        bool chunkFound = false;
        for (int nodeRank : fileMD.chunkPositions[chunkID])
        {
            // checking if nodeRank is active right now
            if (activeNodes.find(nodeRank) != activeNodes.end())
            {
                // setting chunk as found
                chunkFound = true;

                // letting the node know taht download request has come
                MPI_Send(nullptr, 0, MPI_BYTE, nodeRank, DOWNLOAD_REQUEST, MPI_COMM_WORLD);

                // sending chunk ID
                MPI_Send(&chunkID, 1, MPI_LONG_LONG, nodeRank, DOWNLOAD_REQUEST, MPI_COMM_WORLD);

                // receiving chunk data
                char buffer[CHUNK_SIZE];
                MPI_Recv(buffer, CHUNK_SIZE, MPI_CHAR, nodeRank, CHUNK_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // adding to file data
                fileData.append(buffer, CHUNK_SIZE);

                break;
            }
        }

        // checking if chunk was found
        if (!chunkFound)
        {
            return {-1, "File retrieval failed. No active replica found\n"};
        }
    }

    return {1, fileData};
}

void opRetrieveSS(map<int, string> &storedChunks)
{
    // receiving chunkID
    long long int chunkID;
    MPI_Recv(&chunkID, 1, MPI_LONG_LONG, 0, DOWNLOAD_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // sending chunk data
    MPI_Send(storedChunks[chunkID].c_str(), CHUNK_SIZE, MPI_CHAR, 0, CHUNK_DATA, MPI_COMM_WORLD);
}

/*





























*/

bool isWordBoundary(char ch)
{
    return isspace(ch) || ispunct(ch) || ch == '\0';
}

pair<int, vector<size_t>> findWordOccurrences(const string &fileContent, const string &word)
{
    // variables
    vector<size_t> allOffsets;
    size_t currPost = 0;
    int occurCount = 0;
    size_t wordLen = word.length();

    // searching for word
    while ((currPost = fileContent.find(word, currPost)) != string::npos)
    {
        // checking word boundaries
        bool isStartValid = (currPost == 0 || isWordBoundary(fileContent[currPost - 1]));
        bool isEndValid = (currPost + wordLen == fileContent.length() || isWordBoundary(fileContent[currPost + wordLen]));

        if (isStartValid && isEndValid)
        {
            // adding
            allOffsets.push_back(currPost);
            occurCount++;
        }

        // moving to next position
        currPost += wordLen;
    }

    return {occurCount, allOffsets};
}

pair<int, string> opSearchMS(string fileName, string word, map<string, FileMetadata> &metadata, unordered_set<int> &activeNodes)
{
    // handling retrieving file data
    pair<int, string> retVal = opRetrieveMS(fileName, metadata, activeNodes);

    // checking for errors
    if (retVal.first == -1)
    {
        return retVal;
    }

    // handling word count
    pair<int, vector<size_t>> wordOccur = findWordOccurrences(retVal.second, word);

    cout << wordOccur.first << endl;
    for (size_t offset : wordOccur.second)
    {
        cout << offset << " ";
    }
    cout << endl;

    return {1, "Word search successful\n"};
}

/*































*/

pair<int, string> opListFileMS(string fileName, map<string, FileMetadata> &metadata)
{
    // checking if file is present or not
    if (metadata.find(fileName) == metadata.end())
    {
        return {-1, "File not found\n"};
    }

    // getting metadata
    FileMetadata fileMD = metadata[fileName];
    long long int startChunkID = fileMD.startChunkID;

    // iterating over all chunks
    for (int i = 0; i < fileMD.numChunks; i++)
    {
        cout << i << " " << fileMD.chunkPositions[startChunkID + i].size() << " ";
        for (int nodeRank : fileMD.chunkPositions[startChunkID + i])
        {
            cout << nodeRank << " ";
        }
        cout << endl;
    }

    return {1, "File listed successfully\n"};
}

/*

































*/

// MAIN
signed
main(int argc, char *argv[])
{
    // MPI Start
    MPI_Init(&argc, &argv);

    // basic info
    int myRank, numProcesses;
    MPI_Comm_rank(MPI_COMM_WORLD, &myRank);
    MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);

    if (myRank == 0)
    {
        // getting PID
        cout << "Metadata Server ROOT PID: " << getpid() << endl;

        processMetadataServer(numProcesses);
    }
    else
    {
        processStorageNode(myRank);
    }

    // MPI End
    MPI_Finalize();

    return 0;
}