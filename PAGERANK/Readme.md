Implementation of PageRank Algorithm with MapReduce Library


Installation Instructions
1- fork MapReduce Library from https://github.com/jainvasu631/mapreduce
2- Install libboost using Sudo apt install libboost-all-dev on Linux or brew install libboost on MacOs




Compilation Instructions

g++ -std=c++11  -I  PATH_TO_BOOST/include -L PATH_TO_BOOST/lib pageRank.cpp -lboost_filesystem -lboost_system -lboost_iostreams -lpthread -o pageRank.o
update PATH_TO_BOOST above from the path of installation of boost on your system

