#include <iostream>
using namespace std;


int foo(int n){
  std::cout << "1" << std::endl;
  return 2*n;
}

int foo2( int (*f)(int) , int a ){
  return f(a);     
}

void abc(void){
  cout << 2 << endl;
  return ;
}
void bcd(void  (*f)(void)  ){
  f();
  return;
}
int main(){
  cout << foo2(foo,2) << endl;
  bcd(abc);
}
