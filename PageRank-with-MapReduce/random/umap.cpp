#include<cstring>
#include <iostream>
#include<unordered_map>

using namespace std;

int main(){
  
  unordered_map<int,char*> umap;
  char *varray = (char *)malloc(4*10);
  char *key = (char *)malloc(4*10);
  int a=1;
  int b=75;
  int c=3;
  for(int i = 0 ; i < 10 ; i++){
    memcpy(varray+i*4,&a,4);
    memcpy(key+i*4,&b,4);
    a++;
    b=(b+1)%2;
  }
  umap[1] = (char *)malloc(8*sizeof(int));


  memcpy(umap[1],&a,4);
  memcpy(umap[1]+4,&b,4);
  memcpy(umap[1]+8,&c,4);
  cout << *(int *)(umap.find(1)->second+4) << endl;
}
