#include <cstring>
#include <iostream>
#include <vector>
using namespace std;
int main()
{
  int n;
  int max=1;
  n=max=0;
  char *arr = (char *)malloc(2*sizeof(int)*max);
  int a=1;
  for(int i = 0 ; i < 100 ; i++){
    if (n==max){
      char *arr1 = (char *)malloc(2*sizeof(int)*max);
      memcpy(arr1,arr,sizeof(int)*max);
      max=2*max;
      free(arr);
      arr=arr1;
    }
    else
      memcpy(arr+n*sizeof(int),&a,sizeof(int));
    n++;
    a++;
  }

  for(int i = 0 ; i < 100 ; i++)
    cout << *(int *)(arr+4*i) << endl;

}
