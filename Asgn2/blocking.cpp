#include <iostream>
#include <bits/stdc++.h>
#include <mpi.h>
#include <stdio.h> 

#define MASTER_TO_SLAVE_TAG 1
#define SLAVE_TO_MASTER_TAG 5
int min(int a , int b ){
if (a > b )
  return b ;
return a ;
}

void mult(double **A , double ** B , double ** C , int RA , int CA , int CB){
    for(int i = 0 ; i < RA ; i++){
      for(int j = 0 ; j < CB ; j++ ){
        double lsum=0;
        for(int k = 0 ; k < CA; k++){
          lsum+=A[i][k]*B[k][j];
        }
        C[i][j]=lsum;
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
  int comm_sz,my_rank;

  MPI_Init(NULL,NULL);
  MPI_Comm_size(MPI_COMM_WORLD,&comm_sz);
  MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);

  MPI_Barrier(MPI_COMM_WORLD);
  start = MPI_Wtime();

  //Process 0 master , send indices to slave processes
  double **A,**B,**C;
  if (my_rank==0){
  
  int L,U,chunk;
  
  //double **A;
  A = new double*[RA];
  A[0] = new double[RA*CA];
  for(int i = 1; i < RA; i++){
    A[i]=A[i-1]+CA;
  }

  //double **B;
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

  
  printf("A matrix \n");
  for(int i = 0 ; i < RA ; i++){
    for(int j = 0 ; j < CA;j++){
      A[i][j]=(i+2*j)/1000;
      //printf("%d ",(int)A[i][j]);
    }
    //printf("\n");
  }

  printf("B matrix \n");
  for(int i = 0 ; i < RB ; i++){
    for(int j = 0 ; j < CB;j++){
      B[i][j]=i+3*j;
      //printf("%d ",(int)B[i][j]);
    }
    //printf("\n");
  }
    
    /*chunk is the number of rows each process get
     Rows are divided among all the processes equally*/
    chunk = RA/(comm_sz);
    printf("chunk = %d\n",chunk);

    for(int i =1;i<comm_sz;i++){
      L=i*chunk;
      U=L+chunk;
      MPI_Send(&L,1,MPI_INT,i,MASTER_TO_SLAVE_TAG,MPI_COMM_WORLD);
      MPI_Send(&U,1,MPI_INT,i,MASTER_TO_SLAVE_TAG+1,MPI_COMM_WORLD);
      MPI_Send(A[L],CA*chunk,MPI_DOUBLE,i,MASTER_TO_SLAVE_TAG+2,MPI_COMM_WORLD);
      MPI_Send(B[0],CB*RB,MPI_DOUBLE,i,MASTER_TO_SLAVE_TAG+3,MPI_COMM_WORLD);
    }

   
    //process 0 does its share of computation
    mult_block(A,B,C,chunk,CA,CB);
   // for(int i = 0 ; i < chunk ; i++){
   //   for(int j = 0 ; j < CB ; j++ ){
   //     double lsum = 0;
   //     for(int k = 0 ; k < CA; k++){
   //       lsum+=A[i][k]*B[k][j];
   //     }
   //       C[i][j]=lsum;  
   //   }
   // }

    for(int i = 0 ; i < chunk;i++){
      for(int j = 0 ; j < CB ; j++){
        //printf("%d ",(int)C[i][j]);
      }
      //printf("\n");
    }


  }

  else{

    int Ub,Lb; //variable for storing Indices of rows of A and B matrix
    MPI_Status status;


    /*Receiving the start and end indices of rows of A */
    MPI_Recv(&Lb,1,MPI_INT,0,MASTER_TO_SLAVE_TAG,MPI_COMM_WORLD,&status);
    MPI_Recv(&Ub,1,MPI_INT,0,MASTER_TO_SLAVE_TAG+1,MPI_COMM_WORLD,&status);
   // printf("Lb =%d Ub =%d for process %d\n",Lb,Ub,my_rank);

    int chunk = Ub-Lb;
  
    /*Buffer to store rows of A matrix*/
    double **A_LOCAL;
    A_LOCAL = new double*[chunk];
    A_LOCAL[0] = new double[chunk*CA];
    for(int i = 1; i < chunk; i++){
     A_LOCAL[i]=A_LOCAL[i-1]+CA;
    }

    /*Buffer to store B matrix*/
    double **B_LOCAL;
    B_LOCAL = new double*[CB];
    B_LOCAL[0] = new double[RB*CB];
    for(int i = 1; i < RB; i++){
     B_LOCAL[i]=B_LOCAL[i-1]+CB;
    }

    /*Buffer to store local matrix multiplication*/
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


    /*Slave processes receives the rows of A matrix and B matrix*/
    MPI_Recv(A_LOCAL[0],CA*chunk,MPI_DOUBLE,0,MASTER_TO_SLAVE_TAG+2,MPI_COMM_WORLD,&status);
    MPI_Recv(B_LOCAL[0],CB*RB,MPI_DOUBLE,0,MASTER_TO_SLAVE_TAG+3,MPI_COMM_WORLD,&status);


    //code for matrix multiplication
   // for(int i = 0 ; i < chunk ; i++){
   //   for(int j = 0 ; j < CB ; j++ ){
   //     double lsum =0;
   //     for(int k = 0 ; k < CA; k++){
   // //      C_LOCAL[i][j]+=A_LOCAL[i][k]*B_LOCAL[k][j];
   //       lsum+=A_LOCAL[i][k]*B_LOCAL[k][j];
   //     }
   //       C_LOCAL[i][j]=lsum;  
   //   } 
   // }
    mult_block(A_LOCAL,B_LOCAL,C_LOCAL,chunk,CA,CB);
    /*Slave processes send their computation to Master Process 0*/
    MPI_Send(&Lb,1,MPI_INT,0,SLAVE_TO_MASTER_TAG,MPI_COMM_WORLD);
    MPI_Send(&Ub,1,MPI_INT,0,SLAVE_TO_MASTER_TAG+1,MPI_COMM_WORLD);
    MPI_Send(C_LOCAL[0],chunk*CB,MPI_DOUBLE,0,SLAVE_TO_MASTER_TAG+2,MPI_COMM_WORLD);




  }


  //combining the result of slave processes
  if (my_rank==0){
  
    //double **C;
    for(int i = 1 ; i < comm_sz;i++){
      int Lb,Ub;
      MPI_Status status;
      MPI_Recv(&Lb,1,MPI_INT,i,SLAVE_TO_MASTER_TAG,MPI_COMM_WORLD,&status);
      MPI_Recv(&Ub,1,MPI_INT,i,SLAVE_TO_MASTER_TAG+1,MPI_COMM_WORLD,&status);
      MPI_Recv(C[Lb],(Ub-Lb)*CB,MPI_DOUBLE,i,SLAVE_TO_MASTER_TAG+2,MPI_COMM_WORLD,&status);
    }
  

    //print the output
  printf("Final Ouput\n");
  for(int i = 0 ; i < RA ; i++){
    for(int j = 0 ; j < CB ; j++){
      //printf("%d ",(int)C[i][j]);
    }
    //printf("\n");
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
  return 0;
}
