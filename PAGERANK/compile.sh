mpic++ -I mrmpi-7Apr14/src/ -L mrmpi-7Apr14/src/ -o mr-pr-mpi.o mr-pr-mpi.cpp mrmpi-7Apr14/src/libmrmpi_mac.a

mpic++ -I mrmpi-7Apr14/src/ -L mrmpi-7Apr14/src/ -o mr-pr-mpi-base.o mr-pr-mpi-base.cpp mrmpi-7Apr14/src/libmrmpi_mac.a

g++ -I /usr/local/Cellar/boost/1.72.0_1/include -L /usr/local/Cellar/boost/1.72.0_1/lib -o mr-pr-cpp.o mr-pr-cpp.cpp -lboost_filesystem -lboost_system -lboost_iostreams -lpthread
