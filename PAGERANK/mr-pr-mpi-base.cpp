#include "mpi.h"
#include "string.h"
#include "sys/stat.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include <iostream>
#include <vector>
#include <fstream>
#include <unordered_map>

using namespace MAPREDUCE_NS;


int myrank,nprocs;
int n;
double alpha;

std::vector<double> initRank;
std::vector<double> newRank;
std::unordered_map<int,std::vector<int> > LinkMap;

/**First MapReduc Task*/
void mymap(int itask ,KeyValue *kv , void *ptr);
void output(uint64_t itask, char *key, int keybytes, char *value,int valuebytes, KeyValue *kv, void *ptr);
void myreduce(char *key, int keybytes, char *multivalue, int nvalues, int *valuebytes, KeyValue *kv, void *ptr); 

/*Second MapReduce Task*/
void dangling_mapper(int itask ,KeyValue *kv , void *ptr);
void dangling_reducer(char *key,int keybytes,char *multivalue , int nvalues , int *valuebytes,KeyValue *kv, void *ptr);
void dangling_output(uint64_t,char *key,int keybytes,char *value,int valuebytes,KeyValue *kv, void *ptr);

/*Utility Functions*/
bool converge(std::vector<double> initRank , std::vector<double> newRank , double tolerance);
void printEdgeList();
void writeToFile(std::string filename);
double mod(double a);

int main(int narg,char ** args)
{
  
 std::ifstream inFile ;
 inFile.open(args[1]);
 int links;
 alpha=.85;

 //Pages are numbered from 1 to n and not from 0 to n-1
 int a,b;
 n=0;
 while(inFile>> a >> b ){
      
     n=std::max(b,std::max(a,n));
     LinkMap[a].push_back(b);
 }
  n++;
 

  initRank=std::vector<double>(n,0);
  newRank=std::vector<double>(n,1.0/n);
 


  MPI_Init(&narg,&args);
  MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  int iteration=0;
  int nkeys;
  while(!converge(initRank,newRank,.0001/n)){
     initRank=newRank;

     /*Calculation of Matrix Multiplication*/
     MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
     mr->map(n,mymap,NULL);
     mr->collate(NULL);
     mr->reduce(myreduce,NULL);
     
     MPI_Barrier(MPI_COMM_WORLD);

     mr->gather(1); 
     mr->broadcast(0);
     mr->map(mr,output,NULL);
     delete mr;
     
     /*Handling Dangling Pages and Random Surfer Part */
     MapReduce *mr1 = new MapReduce(MPI_COMM_WORLD);
     mr1->map(n,dangling_mapper,NULL);
     mr1->collate(NULL);
     mr1->reduce(dangling_reducer,NULL);
     MPI_Barrier(MPI_COMM_WORLD);
     mr1->gather(1);
     mr1->broadcast(0);
     mr1->map(mr1,dangling_output,NULL);
     delete mr1;
     
     iteration++;
     if (myrank==0)
      std::cout << "iteration " << iteration << std::endl;
  }
  
  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (myrank==0){
   writeToFile(args[2]);
    std::cout << (tstop-tstart)*1000 << std::endl;
   //  for(int i = 0 ; i < n ; i++)
   //  std::cout << "Page " << i << " " <<  newRank[i] << std::endl;
  }
  

  MPI_Finalize();
}

/*Mapper Function for 1 MapReduce Job*/
void mymap(int itask ,KeyValue *kv , void *ptr)
{
  double zero = 0;
  kv->add((char *)&itask,sizeof(int),(char *)&zero,sizeof(double));

  int nLinks = LinkMap[itask].size();
  std::vector<int> LinkVector = LinkMap[itask];

  for(int i = 0 ; i < nLinks;i++){

    double p_i = initRank[itask]/nLinks;
    kv->add((char *)&LinkVector[i],sizeof(int),(char *)&p_i,sizeof(double));
  
  }

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


void output(uint64_t itask, char *key, int keybytes, char *value, int valuebytes, KeyValue *kv, void *ptr)
{
      //std::cout << *(double *)value << std::endl;
      newRank[*(int *)key] = alpha*(*(double *)value);

}

/*Mapper Function for Calculation of Dot Product of Dangling page vector and Initial PageRank
 * Returns (1,p_i) if page i is dangling */
void dangling_mapper(int itask ,KeyValue *kv , void *ptr)
{
  double drank = 0;
  int dKey =0 ; /*Sends all the non zero dot product entries to single reducer*/
  /*Send atleast 1 key for safety purpose*/
  if (itask==1){
    kv->add((char *)&dKey,sizeof(int),(char *)&drank,sizeof(double));
  }
  if (LinkMap[itask].size()==0){
    drank = initRank[itask];
    kv->add((char *)&dKey,sizeof(int),(char *)&drank,sizeof(double));
  }
}

/*Reducer sums up the individual entries of dot product */
void dangling_reducer(char *key,int keybytes,char *multivalue , int nvalues , int *valuebytes,KeyValue *kv, void *ptr)
{
  double dot_product = 0;
  for(int i = 0 ; i < nvalues ; i++){
    double e = *(double *)(multivalue+i*(*valuebytes));
    dot_product+=e;
  }

  kv->add(key,sizeof(int),(char *)&dot_product,sizeof(double));
}

/*updates the newRank vector */
void dangling_output(uint64_t,char *key,int keybytes,char *value,int valuebytes,KeyValue *kv, void *ptr)
{
  for(int i = 0 ; i < n;i++){
    newRank[i]+=alpha*(*(double *)value/n)+(1-alpha)*(1.0/n);
  } 
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


void printEdgeList()
{
    std::unordered_map<int, std::vector<int> >:: iterator p; 


    for(p = LinkMap.begin() ; p != LinkMap.end();p++){
      std ::cout << p->first << " " ; 
      for(int j = 0 ;j < p->second.size();j++){
        std::cout << p->second[j] << " " ;
      }
      std::cout << std::endl;
    }

}


void writeToFile(std::string filename){
  std::ofstream outfile;
  outfile.open(filename);
  double sum =0 ;
  for(int i = 0 ; i < n ; i++){
    sum+=newRank[i];
    outfile << i << " = " << newRank[i] << std::endl;
  }
  outfile << "sum= " << sum << std::endl;
  outfile.close();
}


double mod(double a){
  return (a>0)?a:-1*a;
}
