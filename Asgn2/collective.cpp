#include <iostream>
#include <stdio.h>
#include <bits/stdc++.h>
#include <mpi.h>

int min(int a , int b ){
if (a > b )
  return b ;
return a ;
}
void mult_serial(double **A , double ** B , double ** C , int RA , int CA , int CB){
    for(int i = 0 ; i < RA ; i++){
      for(int j = 0 ; j < CB ; j++ ){
        for(int k = 0 ; k < CA; k++){
          C[i][j]+=A[i][k]*B[k][j];
        }
      }
    }
}
void mult_block(double **A , double ** B , double ** C , int RA , int CA , int CB){
  int g = 16;  
  for(int i0 = 0 ; i0 < RA ; i0+=g){
      for(int j0 = 0 ; j0 < CB ; j0+=g ){
        for(int k0 = 0 ; k0 < CA; k0+=g){
          for(int i = i0 ; i < min(i0+g,RA);i++){
            for(int j =j0 ; j < min(j0+g,CB);j+=8){
              double lsum=0;
              for(int k = k0 ; k <min(k0+g,CA);k++){    
                    C[i][j]+=A[i][k]*B[k][j];
                    C[i][j+1]+=A[i][k]*B[k][j+1];
                    C[i][j+2]+=A[i][k]*B[k][j+2];
                    C[i][j+3]+=A[i][k]*B[k][j+3];
                    C[i][j+4]+=A[i][k]*B[k][j+4];
                    C[i][j+5]+=A[i][k]*B[k][j+5];
                    C[i][j+6]+=A[i][k]*B[k][j+6];
                    C[i][j+7]+=A[i][k]*B[k][j+7];
                    
              }
              //C[i][j]+=lsum;
            }
          }
        }
      }
    }
}
double diff(double **A1, double **A2,int RA , int CA){
  double d = 0;
  for(int i = 0 ; i < RA ; i++){
    for(int j = 0 ; j < CA ; j++){
      d+=abs(A1[i][j]-A2[i][j]);
    }
  }
  return d ;
}
int main(){
int n = 2048;

  int RA,RB,CA,CB;
  RA=RB=CA=CB=n;
  double start , end ;
  int comm_sz, my_rank;

  MPI_Init(NULL,NULL);
  MPI_Comm_size(MPI_COMM_WORLD,&comm_sz);
  MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);


  

  int chunk = RA/comm_sz; //Assuming RA is perfectly divisible by number of process
  double **A_LOCAL,**C_LOCAL,**B;
  A_LOCAL = new double *[chunk];
  C_LOCAL = new double *[chunk];
  A_LOCAL[0] = new double[CA*chunk];
  C_LOCAL[0] = new double[chunk*CB];
  for(int i = 1 ; i < chunk ; i++){
    A_LOCAL[i]=A_LOCAL[i-1]+CA;
    C_LOCAL[i]=C_LOCAL[i-1]+CB;
  }
  
  B = new double *[RB];
  B[0] = new double[RB*CB];
  for(int i=1 ;i < RB ; i++){
    B[i]=B[i-1]+CB;
  }


  double **A,**C ;
  A = new double *[RA];
  C = new double *[RA];


  for(int i=1; i < RA; i++){
    A[i]=A[i-1]+CA;
  }


  MPI_Barrier(MPI_COMM_WORLD);
  start = MPI_Wtime();


  //Process 0 initialises A and B matrix
  if (my_rank==0){
    A[0] = new double[RA*CA];
    C[0] = new double[RA*CB];

    for(int i=1; i < RA; i++){
      A[i]=A[i-1]+CA;
      C[i]=C[i-1]+CB;
    }

    for(int i = 0 ; i < RA ; i++){
      for(int j = 0 ; j < CA ; j++){
        A[i][j]=i+2*j;
      }
    }
    for(int i = 0 ; i < RB ; i++){
      for(int j = 0 ; j < CB ; j++){
        B[i][j]=i+3*j;
      }
    }
  }

  //Scattering the A matrix to all the processes
  MPI_Scatter(A[0],CA*chunk,MPI_DOUBLE,A_LOCAL[0],CA*chunk,MPI_DOUBLE,0,MPI_COMM_WORLD);
  //broadcasting B matrix to all the processes
  MPI_Bcast(B[0],RB*CB,MPI_DOUBLE,0,MPI_COMM_WORLD);


 //  for(int i = 0 ; i < chunk ; i++){
 //   for(int j = 0 ; j < CB ; j++){
 //     for(int k = 0 ; k < CA ; k++){
 //       C_LOCAL[i][j]+=A_LOCAL[i][k]*B[k][j];
 //     }
 //   }
 // }
  mult_block(A_LOCAL,B,C_LOCAL,chunk,RB,CB); 
  //Gathering output at process 0
  MPI_Gather(C_LOCAL[0],chunk*CB,MPI_DOUBLE,C[0],chunk*CB,MPI_DOUBLE,0,MPI_COMM_WORLD);


  //printing the output
  if (my_rank==0){
    for(int i = 0 ; i < RA ; i++){
      for(int j = 0 ; j < CB ; j++){
     //     printf("%d ",(int)C[i][j]);
      }
     // printf("\n");
    }
  }
  
  MPI_Barrier(MPI_COMM_WORLD);
  end = MPI_Wtime();

  if (my_rank ==0){
    printf("%f seconds \n " , end-start);
    double **C_SERIAL;
    C_SERIAL = new double*[RA];
    C_SERIAL[0] = new double[RA*CB];
    for(int i = 1; i < RA; i++){
      C_SERIAL[i]=C_SERIAL[i-1]+CB;
    }

    //mult_serial(A,B,C_SERIAL,RA,CA,CB);
    //double d = diff(C_SERIAL,C,RA,CB);
    //printf("diff is %f\n",d);

  }



  MPI_Finalize();

}
