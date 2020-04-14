#include <mpi.h>
#include <iostream>
#include <vector>
#include <cstring>

int myrank,nprocs;


std::vector<std::vector<int> > inp;
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
    std::memcpy(tempvalueArray,valueArray,max*keybytes);
    
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

class MapReduce
{
  public:
    KeyValue *i_kv; // mapper stores key and value in it
    KeyValue *agr_kv;

    void map(int nmap,void (*mapper)(int,KeyValue*));
    void aggregate();
    int defaultHash(char *key);
    
    MapReduce(){
      i_kv = new KeyValue();
      agr_kv = new KeyValue();
    }

};






void mapper(int itask,KeyValue* kv){
  //std::cout << itask << std::endl;
  for(int i = 0 ; i < inp[itask].size();i++){
    kv->add((char *)&itask,sizeof(int),(char *)&inp[itask][i],sizeof(int));
  }
  return ;
}

int main(){
  for(int i = 0 ; i < 8 ; i++){
    std::vector<int> v;
    for(int j=0; j < 8 ; j++){
      v.push_back(i+j);
    }
    inp.push_back(v);
  } 
  
  MPI_Init(NULL,NULL);
  MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  MapReduce *mr = new MapReduce();
  mr->map(8,mapper);
  mr->aggregate();
  std::cout << mr->agr_kv->getsize() << std::endl ;
  if (myrank==1){
      for(int i = 0 ; i < mr->agr_kv->getsize() ; i++)
        std::cout << *(int *)(mr->agr_kv->getValue(i))  << " ";
  }

  MPI_Finalize();
}


void MapReduce::map(int nmap,void (*mapper)(int,KeyValue*))
{
  //scheduling mapper on different processes
  for(int itask=myrank;itask<nmap;itask+=nprocs){
    mapper(itask,i_kv);
  }
  return ;
}

int MapReduce::defaultHash(char *key){
    return (*key%nprocs+1)%nprocs;
}

//distributes key values with similar key to same processor based on hashed value of key
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
        MPI_Send(i_kv->getKey(keyId),keybytes,MPI_CHAR,pr,i,MPI_COMM_WORLD);
        MPI_Send(i_kv->getValue(keyId),valuebytes,MPI_CHAR,pr,i+key_count,MPI_COMM_WORLD);
      }
    }

    else{
      //if key maps to the same process
      for(int i = 0 ; i < key_count; i++){
        
        int keyId = procmapper[pr][i];
       //add into own aggregate key,value pool
        agr_kv->add(i_kv->getKey(keyId),keybytes,i_kv->getValue(keynum),valuebytes);
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
  
  return ;

}
