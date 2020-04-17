#include <iostream>
#include <vector>
#include <cstring>
#include <unordered_map>
#include <utility>


class KeyMultiValue
{
  
  
  public:
    std::unordered_map<char,std::pair<int,int> > Counter;
    std::unordered_map<char,char*> MultiValueHash;
    void add(char key,int keybytes,char *value,int valuebytes);
    char* getValues(char *key,int keybytes);
    int getValueCount(char *key , int keybytes);
};


int KeyMultiValue::getValueCount(char *key,int keybytes)
{
  return Counter[*key].first;
}

void KeyMultiValue::add(char key,int keybytes,char *value,int valuebytes)
{
  //first entry 
  int n,m;
  if (Counter.find(key) == Counter.end()){
    std::cout << "called " << std::endl;
    Counter[key] = std::pair<int,int>(0,1);
    n=0;
    m=1;
    MultiValueHash[key]=(char *)malloc(valuebytes);
  }
  else{
      n = Counter[key].first ;
      m = Counter[key].second;
      if (n == m){
        char *tempArray = (char *)malloc(2*m*valuebytes);
        memcpy(tempArray,MultiValueHash[key],m*valuebytes);
        m=2*m;
        MultiValueHash[key]=tempArray;
        free(tempArray);
      }
  }
  memcpy(MultiValueHash[key]+n*valuebytes,value,valuebytes);
  n++;
  Counter[key]=std::pair<int,int>(n,m);
  return;
}

char* KeyMultiValue::getValues(char *key,int keybytes){
 // std::cout << "inside getValues " << *(int *)MultiValueHash[*key] << std::endl; 
  return MultiValueHash[*key];
}


int main(){
  
 // KeyMultiValue *kmv = new KeyMultiValue();
  
  char *keyArray = (char *)malloc(10*sizeof(int));
  char *valueArray = (char *)malloc(10*sizeof(int));

  int k,v;
  k=1,v=1;
  for(int i = 0 ; i < 10 ; i++){
    memcpy(keyArray+i*sizeof(int),&k,sizeof(int));
    memcpy(valueArray+i*sizeof(int),&v,sizeof(int));
    v++;
    k=(k+1)%2; 
  }

  for(int i = 0 ; i < 10 ; i++){
    std::cout << *(int *)(keyArray+i*sizeof(int)) << " ";
    std::cout << *(int *)(valueArray+i*sizeof(int)) << std::endl;
  }

  std::unordered_map<int,char *> umap;
  std::unordered_map<int,std::pair<int,int> > count;

  for(int i = 0 ; i < 2 ; i++){
    char *key = keyArray+i*sizeof(int);
    char *value = valueArray+i*sizeof(int);
    int n,m;
    std::cout << "key " << *(int *)key << "value " << *(int *)value << std::endl;
    if (umap[*(int *)key]==NULL){
      n=0;m=1;
      umap[*(int *)key]=(char *)malloc(sizeof(int));
    }
    else
    {
      n = count[*(int *)key].first;
      m = count[*(int *)key].second;
      if (n==m)
      {
        std::cout << "here " << std::endl;
        char *temp = (char*)(malloc(sizeof(int)*2*m) );
        memcpy(temp,umap[*(int *)key],m*sizeof(int));
        umap[*(int *)key]=temp;
        m=2*m;
        free(temp);
      }
    }
    std::cout << n << " " << m << std::endl;
    memcpy(umap[*(int *)key]+n*sizeof(int),value,sizeof(int));
    n++;
    count[*(int *)key]=std::pair<int,int>(n,m);
  }

    
  char *ans = umap[0];
  std::cout << *(int *)(ans) << std::endl;



  //  for(int i = 0 ; i < 4 ; i++){
  //    kmv->add(*(keyArray+i*sizeof(int)),sizeof(int),valueArray+i*sizeof(int),sizeof(int));
  //  }
  //  //for(int i = 0 ; i < 10 ; i++){
  //    //std::cout << "key " << *(int *)(keyArray) << std::endl; 
  //    //std::cout << *(int *)(kmv->MultiValueHash[*keyArray]+i*4) << std::endl;
  //  //}
  //  char *ans = kmv->getValues(keyArray,4);
  //  for(int i=0;i<10;i++){
  //    std::cout << *(int *)(ans+i*4) << std::endl;
  //  }
  //  std::cout << "Map Elements" << std::endl;
  //  for (auto it : kmv->MultiValueHash){
  //    std::cout << +it.first << std::endl;
  //    int count = kmv->Counter[it.first].first ;
  //    std::cout << "count " << count << std::endl;
  //    for(int i = 0 ; i < count ; i++)
  //      std::cout << *(int *)(it.second+i*4) << std::endl;
  //  }
}



