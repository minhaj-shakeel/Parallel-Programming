#include <iostream>
#include <bits/stdc++.h>
#include <mpi.h>
#include <stdio.h> 

#define MASTER_TO_SLAVE_TAG 1
#define SLAVE_TO_MASTER_TAG 5
#define MAX_PROC 16
using namespace std ;

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
void mult_block(double **A , double ** B , double ** C , int RA , int CA , int CB,int p){
  int g = 16;  
  for(int i0 = p ; i0 < RA ; i0+=g){
      for(int j0 = 0 ; j0 < CB ; j0+=g ){
        for(int k0 = 0 ; k0 < CA; k0+=g){
          for(int i = i0 ; i < min(i0+g,RA);i++){
            for(int j =j0 ; j < min(j0+g,CB);j+=8){
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

void printMatrix(double **A , int RA , int CA){
  for(int i = 0 ; i < RA ; i++){
    for(int j = 0 ; j < CA ; j++ ){
      cout << A[i][j] << " " ; 
    }
    cout << endl ;
  }
}

int main(){
  int n = 4096 ;
  int comm_sz,my_rank;
  int RA,RB,CA,CB;
  RA=RB=CA=CB=n;
  double start,end;


  MPI_Init(NULL,NULL);
  MPI_Comm_size(MPI_COMM_WORLD,&comm_sz);
  MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);
  
  MPI_Request L_req[MAX_PROC],U_req[MAX_PROC],C_req[MAX_PROC],A_req,B_req;
  


  MPI_Barrier(MPI_COMM_WORLD);
  start = MPI_Wtime();

  double **A,**B,**C;
  //Process 0 master , send indices to slave processes
  if (my_rank==0){
  
  int L,U,chunk;
  MPI_Request request;
  
  A = new double*[RA];
  A[0] = new double[RA*CA];
  for(int i = 1; i < RA; i++){
    A[i]=A[i-1]+CA;
  }

  B = new double*[RB];
  B[0] = new double[RB*CB];
  for(int i = 1; i < RB; i++){
    B[i]=B[i-1]+CB;
  }

  C = new double*[RA];
  C[0] = new double[RA*CB];
  for(int i = 1; i < RA; i++){
    C[i]=C[i-1]+CB;
  }

  printf("Initialising A matrix \n");
  for(int i = 0 ; i < RA ; i++){
    for(int j = 0 ; j < CA;j++){
      A[i][j]=(i+2*j)/100;
    }
  }

  printf("Initialising B matrix \n");
  for(int i = 0 ; i < RB ; i++){
    for(int j = 0 ; j < CB;j++){
      B[i][j]=(i+3*j)/100;
    }
  }

    /*chunk is the number of rows each process get
     Rows are divided among all the processes equally*/
    chunk = RA/(comm_sz);
    //printf("chunk = %d\n",chunk);
    int numblock = chunk/16;
    //printf("numblock=%d\n",numblock);

    for(int i =1;i<comm_sz;i++){
      L=(i)*chunk;
      U=L+chunk;
      MPI_Isend(&L,1,MPI_INT,i,MASTER_TO_SLAVE_TAG,MPI_COMM_WORLD,&request);
      MPI_Isend(&U,1,MPI_INT,i,MASTER_TO_SLAVE_TAG+1,MPI_COMM_WORLD,&request);
      MPI_Isend(B[0],CB*RB,MPI_DOUBLE,i,MASTER_TO_SLAVE_TAG+2,MPI_COMM_WORLD,&request); 
      
      for (int b = 0 ; b < numblock ; b++){
        MPI_Isend(A[L+b*16],CA*16,MPI_DOUBLE,i,MASTER_TO_SLAVE_TAG+3+b,MPI_COMM_WORLD,&request);
      }
    }
    mult_block(A,B,C,chunk,CA,CB,0); 
  }

  else{
    int Ub,Lb;
    MPI_Status status;
    
    MPI_Irecv(&Lb,1,MPI_INT,0,MASTER_TO_SLAVE_TAG,MPI_COMM_WORLD,&L_req[0]);
    MPI_Irecv(&Ub,1,MPI_INT,0,MASTER_TO_SLAVE_TAG+1,MPI_COMM_WORLD,&U_req[0]);
    
    MPI_Wait(&L_req[0],NULL);
    MPI_Wait(&U_req[0],NULL);

    int chunk = Ub-Lb;
  
    double **A_LOCAL;
    A_LOCAL = new double*[chunk];
    A_LOCAL[0] = new double[chunk*CA];
    for(int i = 1; i < chunk; i++){
     A_LOCAL[i]=A_LOCAL[i-1]+CA;
    }

    double **B_LOCAL;
    B_LOCAL = new double*[CB];
    B_LOCAL[0] = new double[RB*CB];
    for(int i = 1; i < RB; i++){
     B_LOCAL[i]=B_LOCAL[i-1]+CB;
    }

    
    double **C_LOCAL;
    C_LOCAL = new double*[chunk];
    C_LOCAL[0] = new double[chunk*CB];
    for(int i = 1; i < chunk; i++){
     C_LOCAL[i]=C_LOCAL[i-1]+CB;
    }
  
    for(int i = 0 ; i < chunk ; i++){
      for(int j = 0 ; j < CB ; j++){
        C_LOCAL[i][j]=0;
      }
    }


    int numblocks = chunk/16;
    MPI_Request Areq[numblocks];
    MPI_Irecv(B_LOCAL[0],CB*RB,MPI_DOUBLE,0,MASTER_TO_SLAVE_TAG+2,MPI_COMM_WORLD,&B_req);
    
    for (int i = 0 ; i < numblocks ; i++){ 
      MPI_Irecv(A_LOCAL[0+16*i],CA*16,MPI_DOUBLE,0,MASTER_TO_SLAVE_TAG+3+i,MPI_COMM_WORLD,&Areq[i]);
    }


    MPI_Wait(&B_req,NULL);
    for(int i = 0 ; i < numblocks;i++){ 
      MPI_Wait(&Areq[i],NULL);
      mult_block(A_LOCAL,B_LOCAL,C_LOCAL,16,CA,CB,i*16); 
    }
    
    MPI_Request request;
    MPI_Isend(&Lb,1,MPI_INT,0,SLAVE_TO_MASTER_TAG,MPI_COMM_WORLD,&request);
    MPI_Isend(&Ub,1,MPI_INT,0,SLAVE_TO_MASTER_TAG+1,MPI_COMM_WORLD,&request);
    MPI_Isend(C_LOCAL[0],chunk*CB,MPI_DOUBLE,0,SLAVE_TO_MASTER_TAG+2,MPI_COMM_WORLD,&request);




  }


  //combining the result of slave processes
  if (my_rank==0){
  
    //double **C;
    for(int i = 1 ; i < comm_sz;i++){
      int Lb,Ub;
      MPI_Status status; 
      MPI_Irecv(&Lb,1,MPI_INT,i,SLAVE_TO_MASTER_TAG,MPI_COMM_WORLD,&L_req[i]);
      MPI_Irecv(&Ub,1,MPI_INT,i,SLAVE_TO_MASTER_TAG+1,MPI_COMM_WORLD,&U_req[i]);
      MPI_Wait(&L_req[i],NULL);
      MPI_Wait(&U_req[i],NULL);
      MPI_Irecv(C[Lb],(Ub-Lb)*CB,MPI_DOUBLE,i,SLAVE_TO_MASTER_TAG+2,MPI_COMM_WORLD,&C_req[i]);
    }
  


  }

  if (my_rank==0){

    for(int i = 1 ; i < comm_sz;i++){
      MPI_Wait(&C_req[i],NULL);
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

    for(int i = 0 ; i < RA ; i++){
      for(int j = 0 ; j < CB ; j++){
        C_SERIAL[i][j] =0 ;
      }
    }
  

    //mult_serial(A,B,C_SERIAL,RA,CA,CB);
    //double d = diff(C_SERIAL,C,RA,CB);
    // printf("diff is %f\n",d);

  }

  MPI_Finalize();
  return 0;
}
