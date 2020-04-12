#include "mpi.h"
#include "string.h"
#include "sys/stat.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include <iostream>
#include <vector>
#include <fstream>

using namespace MAPREDUCE_NS;

int myrank,nprocs;
int n;
double alpha;
std::vector<std::vector<int> > EdgeList;
std::vector<double> initRank;
std::vector<double> newRank;
std::vector<double> danglingRank;

void printEdgeList()
{
    if (myrank!=0)
      return;
    for(int i = 0 ; i < EdgeList.size();i++){
      for(int j = 0 ;j < EdgeList[i].size();j++){
        std::cout << EdgeList[i][j] << " " ;
      }
      std::cout << std::endl;
    }

}

void mymap(int itask ,KeyValue *kv , void *ptr)
{
  double zero = 0;
  kv->add((char *)&itask,sizeof(int),(char *)&zero,sizeof(double));
  for(int i = 1 ; i <= EdgeList[itask][0];i++){
    int numj = EdgeList[itask][0];
    double p_i = initRank[itask]/numj;
    kv->add((char *)&EdgeList[itask][i],sizeof(int),(char *)&p_i,sizeof(double));
  }
}
void dangling_mapper(int itask ,KeyValue *kv , void *ptr)
{
  double drank = 0;
  int dKey =0 ; /*Sends all the non zero dot product entries to single reducer*/
  /*Send atleast 1 key for safety purpose*/
  if (itask==1){
    kv->add((char *)&dKey,sizeof(int),(char *)&drank,sizeof(double));
  }
  if (EdgeList[itask][0]==0){
    drank = initRank[itask];
    kv->add((char *)&dKey,sizeof(int),(char *)&drank,sizeof(double));
  }
}
void dangling_reducer(char *key,int keybytes,char *multivalue , int nvalues , int *valuebytes,KeyValue *kv, void *ptr)
{
  double dot_product = 0;
  for(int i = 0 ; i < nvalues ; i++){
    double e = *(double *)(multivalue+i*(*valuebytes));
    dot_product+=e;
  }

  kv->add(key,sizeof(int),(char *)&dot_product,sizeof(double));
}
void dangling_output(uint64_t,char *key,int keybytes,char *value,int valuebytes,KeyValue *kv, void *ptr)
{
  for(int i = 0 ; i < danglingRank.size();i++){
    danglingRank[i]=*(double *)value/n;
    newRank[i]+=alpha*(*(double *)value/n)+(1-alpha)*(1.0/n);
  } 
}
void output(uint64_t itask, char *key, int keybytes, char *value,
	    int valuebytes, KeyValue *kv, void *ptr)
{
      //std::cout << *(double *)value << std::endl;
      newRank[*(int *)key] = alpha*(*(double *)value);

}
void myreduce(char *key, int keybytes, char *multivalue, int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  double sum = 0;
  for(int i = 0 ; i < nvalues ; i++){
      double r = *(double *)(multivalue+i*(*valuebytes));
      sum+=r;
  }
  kv->add(key,keybytes,(char *)&sum,sizeof(double));
}
double mod(double a){
  return (a>0)?a:-1*a;
}
bool converge(std::vector<double> initRank , std::vector<double> newRank , double tolerance){
  /*Difference in newly computed pageRank and previous pageRank 
   * for any page must be smaller than tolerance limit */  
  for(int i = 0 ; i < initRank.size(); i++){
          if (mod(initRank[i]-newRank[i]) > tolerance){
            return false;
          }
    }
    return true;
      
}
int main(int narg,char ** args)
{
  
 std::ifstream inFile ;
 inFile.open(args[1]);
 int links;
 alpha=.85;
 inFile >> n;
 inFile >> links;

 //Pages are numbered from 1 to n and not from 0 to n-1
 EdgeList=std::vector<std::vector<int> >(n,std::vector<int>(1,0));
 danglingRank=initRank=std::vector<double>(n,0);
 newRank=std::vector<double>(n,1.0/n);
 for(int i = 0 ; i < links ; i++){
     int a,b;
     inFile >> a >>  b ;
     EdgeList[a-1][0]++;
     EdgeList[a-1].push_back(b-1);
 }

  MPI_Init(&narg,&args);
  MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  //mr->verbosity = 2;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  int iteration=0;
  int nkeys;
  while(!converge(initRank,newRank,.0001)){
     initRank=newRank;
     MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
     mr->map(n,mymap,NULL);
     mr->collate(NULL);
     mr->reduce(myreduce,NULL);

     MPI_Barrier(MPI_COMM_WORLD);

     mr->gather(1); 
     mr->broadcast(0);
     mr->map(mr,output,NULL);
     delete mr;
     iteration++;
     MapReduce *mr1 = new MapReduce(MPI_COMM_WORLD);
     mr1->map(n,dangling_mapper,NULL);
     mr1->collate(NULL);
     mr1->reduce(dangling_reducer,NULL);
     MPI_Barrier(MPI_COMM_WORLD);
     mr1->gather(1);
     mr1->broadcast(0);
     mr1->map(mr1,dangling_output,NULL);
     delete mr1;
     
     
  }



  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

 if (myrank==0){
 for(int i = 0 ; i < n ; i++)
   std::cout << "Page " << i << " " <<  newRank[i] << std::endl;
 }
  MPI_Finalize();
}

