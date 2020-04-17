#include <iostream>
#include <cstring>
#include <utility>
#include <unordered_map>
#include <map>

using namespace std;

struct darray{
  size_t size;
  size_t capacity;
  int databytes;
  char *data;
  darray(int databytes_){
    databytes = databytes_;
    size=0;
    capacity = 1;
    data = (char *)(malloc(databytes));
  }
  void add(char *value){
    if (size==capacity){
      char *temp = (char *)malloc(2*capacity*databytes);
      memcpy(temp,data,capacity*databytes);
      free(data);
      data = temp;
      capacity=2*capacity;
    }
    memcpy(data+size*databytes,value,databytes);
    size++;
    return;
  }
};

int main(){
  struct darray *arr0 = new darray(sizeof(int)) ;
  struct darray *arr1 = new darray(sizeof(int)) ;
  cout << arr0 << endl;
  cout << arr1 << endl;
  map<int,struct darray> umap;
  char *valueArray = (char *)malloc(sizeof(int)*10);
  int a = 0;
  for(int i = 0 ; i < 10 ; i++){
    memcpy(valueArray+i*sizeof(int),&a,sizeof(int));
    a++;
  }

  
  int key = 0 ;
  for(int i = 0 ; i< 10 ; i++){
    if (key==0)
      arr0->add(valueArray+i*sizeof(int));
    else
      arr1->add(valueArray+i*sizeof(int));
    key=(key+1)%2;
  }
  for(int i = 0 ; i < arr0->size ; i++){
      cout << *(int *)(arr0->data+i*sizeof(int)) << " ";
      cout << endl;
  }
  for(int i = 0 ; i < arr1->size ; i++){
      cout << *(int *)(arr1->data+i*sizeof(int)) << " ";
      cout << endl;
  }
  

}
