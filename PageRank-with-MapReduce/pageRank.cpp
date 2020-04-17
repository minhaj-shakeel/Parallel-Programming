#include <boost/config.hpp>
#if defined(BOOST_MSVC)
#   pragma warning(disable: 4127)

// turn off checked iterators to avoid performance hit
#   if !defined(__SGI_STL_PORT)  &&  !defined(_DEBUG)
#       define _SECURE_SCL 0
#       define _HAS_ITERATOR_DEBUGGING 0
#   endif
#endif

#include "mapreduce/include/mapreduce.hpp"


#include <iostream>
#include <fstream>
#include <utility>
#include <numeric>



namespace pageRank{

  std::vector<std::vector<double> > Edges ;
  std::vector<double> rank;

  template<typename  MapTask>
  class datasource : mapreduce::detail::noncopyable
  {
    public:
      datasource(long size) : sequence_(1),size_(size) 
     {
     }
     bool const setup_key(typename MapTask::key_type &key)
     {
       key = sequence_++; //pages are numbered from 1 to n

       return key <= size_;
     }

     bool const get_data(typename MapTask::key_type const &key , typename MapTask::value_type &value)
     {
       /*Pass a Pair as value to Mapper function per page 
       Key is Page id
       first argument of value pair is the pageRank of that page
       second argument of value contains a vector whose first entry
       contains the number of pages it refer to and rest
       are the corresponding page ids it links to. 
       Example  <2,<.10,{3,2,4,5}>> means page id = 2, 
                                    its pageRank is .10 
                                    and it links to 3 pages whose ids are 2,4,5
       */
       value.first = rank[key-1];
       value.second = Edges[key-1];
       return true;
     }
    private:
     unsigned sequence_;
     long const size_;
  };



  struct map_task : public mapreduce::map_task<double, std::pair<double , std::vector<double> > >
  {
    template<typename Runtime>
    void operator()(Runtime &runtime,key_type const &key , value_type const &value) const
    {

      /*Mapper returns the pageId and corresponding increase 
       in its pageRank because of page whose pageId= key links to it*/
      runtime.emit_intermediate(key,0);
      for(int i = 1 ; i <= value.second[0]; i++){
        runtime.emit_intermediate(value.second[i],value.first/value.second[0]);
      }
    }
  };                 
  
  struct reduce_task : public mapreduce::reduce_task<double , double >
  {  
    template<typename Runtime , typename It>
    void operator()(Runtime &runtime , key_type const &key , It it , It const  ite) const
    {
        /*It receieves pageId as key and its values of new rank 
         * because of pages links to it and it returns 
         * the sum of these values as new pageRank */

        double sum = 0 ;
        for(It it1 = it ; it1 != ite ; it1++)
          sum+=*it1 ;
        runtime.emit(key,sum);
    }

  };

  typedef 
  mapreduce::job<pageRank::map_task,
                 pageRank::reduce_task,
                 mapreduce::null_combiner,
                 pageRank::datasource<pageRank::map_task>
  >job;

} //namespace pageRank


bool converge(std::vector<double> initRank , std::vector<double> newRank , double tolerance){
  /*Difference in newly computed pageRank and previous pageRank 
   * for any page must be smaller than tolerance limit */  
  for(int i = 0 ; i < initRank.size(); i++){
          if (std::abs(initRank[i]-newRank[i]) > tolerance){
            return false;
          }
    }
    return true;
      
}


int main(int argc , char **argv){
  /*taking input from a file in which first 2 integers represent 
   * the number of pages and number of links 
   * and subsequent lines have pair of intgers 
   * representing edge from page1 to page2 */




  std::ifstream inFile ;
  inFile.open(argv[1]);
  int n,links;
  inFile >> n;
  inFile >> links;

  //Pages are numbered from 1 to n and not from 0 to n-1
  std::vector<std::vector<double> > Edgelist(n,std::vector<double>(1,0));
  for(int i = 0 ; i < links ; i++){
      int a,b;
      inFile >> a >>  b ;
      Edgelist[a-1][0]++;
      Edgelist[a-1].push_back(b);
  }
  
  /*Initilial pageRanks are stored in initRank and newly computed in newRank*/
  std::vector<double> initRank(n,0);
  std::vector<double> newRank(n,1.0/n);

  //Storing the Edges 
  pageRank::Edges = Edgelist;

  int iteration = 0; //to store the number of count till convergence
  while(!converge(initRank,newRank,.0001)){
     iteration++;
     initRank = newRank;
     pageRank::rank = initRank;
     mapreduce::specification spec;
     pageRank::job::datasource_type datasource(8);
     spec.map_tasks = 1;
     spec.reduce_tasks=std::max(1U,std::thread::hardware_concurrency());

     pageRank::job job(datasource,spec);
     
     mapreduce::results result;
   # ifdef _DEBUG 
     job.run<mapreduce::schedule_policy::sequential<pageRank::job> >(result);
   # else 
     job.run<mapreduce::schedule_policy::cpu_parallel<pageRank::job> >(result);
   # endif
     
     for(auto it=job.begin_results();it!=job.end_results();++it)
     {
       newRank[it->first-1]=it->second;
     }
  }

  std::cout << "Number of iterations " << iteration << std::endl;
  
  for(int i = 0 ; i < n ; i++){
    std::cout << "Page " <<  i+1 << " " <<  newRank[i] << std::endl;
  }
  
}

