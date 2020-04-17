#include <mpi.h>
#include <iostream>
#include <vector>
#include <cstring>
#include <unordered_map>
#include <utility>
#include <fstream>


/*Global Data Structures For Computing PageRank*/
int myrank,nprocs;
int n;
double alpha;
std::vector<double> initRank;
std::vector<double> newRank;
std::unordered_map<int,std::vector<int> > LinkMap;

class KeyValue
{
  public:
    char *keyArray;
    char *valueArray;
    int n,max;
    int keylength,valuelength;
    void add(char *key,int keybytes,char *value,int valuebytes);
    KeyValue(){
      n=max=0;
    }
    char *getKey(int num);
    char *getValue(int num);
    int getsize(){
      return n;
    }

};


class KeyMultiValue
{
  
  
  public:
    int keybytes;
    int valuebytes;
    std::unordered_map<int,std::pair<int,int> > Counter;
    std::unordered_map<int,char*> MultiValueHash;
    void add(char *key,int keybytes,char *value,int valuebytes);
    char* getValues(char *key,int keybytes);
    int getValueCount(char *key , int keybytes);
};


class MapReduce
{
  public:
    KeyValue *i_kv; // mapper stores key and value in it
    KeyValue *agr_kv;
    KeyMultiValue *kmv;    
    void map(int nmap,void (*mapper)(int,KeyValue*));
    void map(MapReduce* mr,void (*mapper)(int,char*,int,char*,int,KeyValue*));
    void reduce(void (*reducer)(char*,int,char*,int,int,KeyValue*));
    void aggregate();
    void convert();
    void gather(int pid);
    void broadcast(int pid);
    int defaultHash(char *key);
     
    MapReduce(){
      i_kv = new KeyValue();
      agr_kv = new KeyValue();
      kmv = new KeyMultiValue();
    }

};

bool converge(std::vector<double> initRank , std::vector<double> newRank , double tolerance);
double mod(double a);

/*1st MapReduce Job*/
void mymap(int itask ,KeyValue *kv);
void myreduce(char *key, int keybytes, char *multivalue, int nvalues, int valuebytes, KeyValue *kv); 
void output(int itask, char *key, int keybytes, char *value, int valuebytes, KeyValue *kv);

int main(int narg,char **args){
 std::ifstream inFile ;
 inFile.open(args[1]);
 int links;
 alpha=.85;

 int a,b;
 n=0;
 while(inFile>> a >> b ){
     a--;b--; 
     n=std::max(b,std::max(a,n));
     LinkMap[a].push_back(b);
 }
  n++;

  initRank=std::vector<double>(n,0);
  newRank=std::vector<double>(n,1.0/n);


  MPI_Init(NULL,NULL);
  MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  int iteration = 0;
  //while(!converge(initRank,newRank,.0001)){
  while(true){
     initRank=newRank;
     MapReduce *mr = new MapReduce();
     mr->map(n,mymap);
     mr->aggregate();
     mr->convert();
     mr->reduce(myreduce);
     MPI_Barrier(MPI_COMM_WORLD);

     mr->gather(1);
     MPI_Barrier(MPI_COMM_WORLD);
     mr->broadcast(1);
     MPI_Barrier(MPI_COMM_WORLD);

     mr->map(mr,output);
     MPI_Barrier(MPI_COMM_WORLD);

     //delete mr;
     iteration++;
     if (iteration == 2)
      break;
  }
  if (myrank==0){
      for (int i = 0 ; i < n ; i++)
        std::cout << newRank[i] << std::endl;
  }
 // 
  MPI_Finalize();
}

/*Mapper Function for 1st MapReduce Job*/
void mymap(int itask ,KeyValue *kv)
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

/*Reducer Function for 1st MapReduce Job*/
void myreduce(char *key, int keybytes, char *multivalue, int nvalues, int valuebytes, KeyValue *kv) 
{
  double sum = 0;
  for(int i = 0 ; i < nvalues ; i++){
      double r = *(double *)(multivalue+i*(valuebytes));
      sum+=r;
  }
  kv->add(key,keybytes,(char *)&sum,sizeof(double));
}

void output(int itask, char *key, int keybytes, char *value, int valuebytes, KeyValue *kv){
      //std::cout << *(double *)value << std::endl;
      newRank[*(int *)key] = (*(double *)value);

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

double mod(double a){
  if (a>=0)
    return a;
  return -1*a;
}

/*Returns intermediate Keyvalue Pairs*/
void MapReduce::map(MapReduce *mr,void (*mapper)(int,char*,int,char*,int,KeyValue*)){
    KeyValue *n_kv = new KeyValue();
    int nkeys = mr->i_kv->getsize();
    for(int i = 0 ; i < nkeys ; i++){
      mapper(i,mr->i_kv->getKey(i),mr->i_kv->keylength,mr->i_kv->getValue(i),mr->i_kv->valuelength,n_kv);
    }
    free(mr->i_kv);
    mr->i_kv = n_kv;
}

void MapReduce::map(int nmap,void (*mapper)(int,KeyValue*))
{
  //scheduling mapper on different processes
  for(int itask=myrank;itask<nmap;itask+=nprocs){
    mapper(itask,i_kv);
  }
  return ;
}

/*distributes key values with similar key to same processor based on hashed value of key */
void MapReduce::aggregate()
{
  //Metadata about key value
  int nkeys = i_kv->getsize();
  int keybytes = i_kv->keylength;
  int valuebytes = i_kv->valuelength;
 
  //Stores the mapping of key to process
  std::vector<std::vector<int> > procmapper(nprocs,std::vector<int>(0));
  for(int i = 0 ; i < nkeys;i++){
    int assigned_proc = defaultHash(i_kv->getKey(i));
    procmapper[assigned_proc].push_back(i);
  }



  //Sending keyValues to all the processes
  for(int pr = 0 ; pr < nprocs ; pr++){
    //number of keys to be send to process
    int key_count = procmapper[pr].size();
    
    if( pr!=myrank){
      //first send the number of key,value of pairs to be send
      MPI_Send(&key_count,1,MPI_INT,pr,0,MPI_COMM_WORLD);
      for(int i = 0 ; i < key_count ; i++){
        int keyId = procmapper[pr][i];
        //send the key value in blocking fashion
        //std::cout << *(int *)i_kv->getKey(keyId) << " ";
        //std::cout << *(double *)i_kv->getValue(keyId) << std::endl;
        MPI_Send(i_kv->getKey(keyId),keybytes,MPI_CHAR,pr,i,MPI_COMM_WORLD);
        MPI_Send(i_kv->getValue(keyId),valuebytes,MPI_CHAR,pr,i+key_count,MPI_COMM_WORLD);
      }
    }

    else{
      //if key maps to the same process
      for(int i = 0 ; i < key_count; i++){
        
        int keyId = procmapper[pr][i];
       //add into own aggregate key,value pool
        agr_kv->add(i_kv->getKey(keyId),keybytes,i_kv->getValue(keyId),valuebytes);
      }
    }
  }

  //Receiving Key Values from other processes

    char *keyBuffer = (char *)malloc(keybytes);
    char *valueBuffer = (char *)malloc(valuebytes);
    int to_recv;
    
    for(int rproc = 0 ; rproc  < nprocs ; rproc++ ){
      if (rproc!=myrank){
        MPI_Recv(&to_recv,1,MPI_INT,rproc,0,MPI_COMM_WORLD,NULL);
        for(int j = 0 ; j < to_recv ; j++){
          MPI_Recv(keyBuffer,keybytes,MPI_CHAR,rproc,j,MPI_COMM_WORLD,NULL);
          MPI_Recv(valueBuffer,valuebytes,MPI_CHAR,rproc,j+to_recv,MPI_COMM_WORLD,NULL);
          
          agr_kv->add(keyBuffer,keybytes,valueBuffer,valuebytes);
        }
      }

    }
  free(i_kv); 
  return ;

}

/*convert aggregated key value to key multivalue pair*/
void MapReduce::convert(){
  
  int nKeys = agr_kv->getsize();
  
  int keybytes = agr_kv->keylength;
  int valuebytes = agr_kv->valuelength;
  for(int i = 0 ; i < nKeys ; i++){
    kmv->add(agr_kv->getKey(i),keybytes,agr_kv->getValue(i),valuebytes);
  }
  free(agr_kv);
  return;
}


void MapReduce::reduce(void (*reducer)(char*,int,char*,int,int,KeyValue*)){
    i_kv = new KeyValue();
    for(auto it :kmv->Counter){
      char* varray = kmv->MultiValueHash[it.first];
      int nvalues = it.second.first;
      reducer((char *)&(it.first),kmv->keybytes,varray,nvalues,kmv->valuebytes,i_kv);
    } 
}

int MapReduce::defaultHash(char *key){
    return (*key%nprocs+1)%nprocs;
}

/* Gathers all key value pairs to one process */
void MapReduce::gather(int pid){
  
  int keybytes = i_kv->keylength;
  int valuebytes = i_kv->valuelength;
 
  if (myrank!=pid){
     int key_count = i_kv->getsize();

     MPI_Send(&key_count,1,MPI_INT,pid,0,MPI_COMM_WORLD);
     for(int i = 0 ; i < key_count ; i++){
       MPI_Send(i_kv->getKey(i),keybytes,MPI_CHAR,pid,i,MPI_COMM_WORLD);
       MPI_Send(i_kv->getValue(i),valuebytes,MPI_CHAR,pid,i+key_count,MPI_COMM_WORLD);
     }
  }

  else{
    //target Process gather all the key values
      char *keyBuffer = (char*)malloc(keybytes);
      char *valueBuffer = (char*)malloc(valuebytes);
      int to_recv;
      for(int rproc = 0 ; rproc < nprocs ; rproc++){
      if (rproc!=pid){
        MPI_Recv(&to_recv,1,MPI_INT,rproc,0,MPI_COMM_WORLD,NULL);
        for(int j = 0 ; j < to_recv ; j++){
          MPI_Recv(keyBuffer,keybytes,MPI_CHAR,rproc,j,MPI_COMM_WORLD,NULL);
          MPI_Recv(valueBuffer,valuebytes,MPI_CHAR,rproc,j+to_recv,MPI_COMM_WORLD,NULL);
          i_kv->add(keyBuffer,keybytes,valueBuffer,valuebytes);
        }
      }
    }
  }
  return;
}

/*Broadcast all key values from given process to all the process*/
void MapReduce::broadcast(int pid){
  
  int keybytes ;
  int valuebytes ;
  int key_count ;
  if (myrank==pid){
    
    keybytes = i_kv->keylength;
    valuebytes = i_kv->valuelength;
    key_count = i_kv->getsize();
    std::cout << key_count << std::endl;
    
    /*Broadcasting kv metadata*/
    MPI_Bcast(&key_count,1,MPI_INT,pid,MPI_COMM_WORLD);
    MPI_Bcast(&keybytes,1,MPI_INT,pid,MPI_COMM_WORLD);
    MPI_Bcast(&valuebytes,1,MPI_INT,pid,MPI_COMM_WORLD);
    
    /*Broadcasting Pointer to Key,Value array*/
    MPI_Bcast(i_kv->getKey(0),keybytes*key_count,MPI_CHAR,pid,MPI_COMM_WORLD);
    MPI_Bcast(i_kv->getValue(0),valuebytes*key_count,MPI_CHAR,pid,MPI_COMM_WORLD);

  }
  else{
    char *keyBuffer = (char*)malloc(key_count*keybytes);
    char *valueBuffer = (char*)malloc(key_count*valuebytes);

    MPI_Bcast(&key_count,1,MPI_INT,pid,MPI_COMM_WORLD);
    MPI_Bcast(&keybytes,1,MPI_INT,pid,MPI_COMM_WORLD);
    MPI_Bcast(&valuebytes,1,MPI_INT,pid,MPI_COMM_WORLD);
    
    MPI_Bcast(keyBuffer,keybytes*key_count,MPI_CHAR,pid,MPI_COMM_WORLD);
    MPI_Bcast(valueBuffer,valuebytes*key_count,MPI_CHAR,pid,MPI_COMM_WORLD);
    
    /*Deleting all the existing key values*/
    free(i_kv);
    i_kv=new KeyValue();
    
    /*Inserting all the received key values one by one 
     * Can be optimised by multiadding*/

    for(int i = 0 ; i < key_count ; i++){
      i_kv->add(keyBuffer+i*keybytes,keybytes,valueBuffer+i*valuebytes,valuebytes);
    }
    
    free(keyBuffer);
    free(valueBuffer);

  }
  return ;
}



/*KeyValue Class*/
char * KeyValue::getKey(int num){
  return keyArray+num*keylength;
}

char * KeyValue::getValue(int num){
  return valueArray+num*valuelength;
}

void KeyValue::add(char *key,int keybytes, char *value,int valuebytes){
  if (max==0){
    keyArray = (char *)std::malloc(keybytes);
    valueArray = (char *)std::malloc(valuebytes);
   
    keylength = keybytes;
    valuelength = valuebytes;
   
    max++;
  }
  if (n==max){
    
    char *tempkeyArray = (char *)std::malloc(2*max*keybytes);
    char *tempvalueArray = (char *)std::malloc(2*max*valuebytes);
    
    std::memcpy(tempkeyArray,keyArray,max*keybytes);
    std::memcpy(tempvalueArray,valueArray,max*valuebytes);
    
    max=2*max;
    
    free(keyArray);
    free(valueArray);

    keyArray=tempkeyArray;
    valueArray=tempvalueArray;

  }
   
  std::memcpy(keyArray+n*keybytes,key,keybytes);
  std::memcpy(valueArray+n*valuebytes,value,valuebytes);
  
  n++;
}

/*KeyMultivalue Class*/
void KeyMultiValue::add(char *key,int keybytes_,char *value,int valuebytes_)
{
  //first entry 
  int n,m;
  if (Counter.find(*key) == Counter.end()){
    keybytes=keybytes_;
    valuebytes=valuebytes_;
    Counter[*(int *)key] = std::pair<int,int>(0,1);
    n=0;
    m=1;
    MultiValueHash[*(int *)key]=(char *)malloc(valuebytes);
  }
  else{
      n = Counter[*key].first ;
      m = Counter[*key].second;
      if (n == m){
        char *tempArray = (char *)malloc(2*m*valuebytes);
        memcpy(tempArray,MultiValueHash[*key],m*valuebytes);
        m=2*m;
        MultiValueHash[*(int *)key]=tempArray;
      }
  }
  memcpy(MultiValueHash[*(int *)key]+n*valuebytes_,value,valuebytes);
  n++;
  Counter[*(int *)key]=std::pair<int,int>(n,m);
  return;
}

int KeyMultiValue::getValueCount(char *key,int keybytes)
{
  return Counter[*(int *)key].first;
}

char* KeyMultiValue::getValues(char *key,int keybytes){
  return MultiValueHash[*(int *)key];
}

