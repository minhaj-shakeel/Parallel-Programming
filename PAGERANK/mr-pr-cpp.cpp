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
#include <unordered_map>


namespace pageRank{

  std::vector<std::vector<int> > Edges ;
  std::unordered_map<int,std::vector<int> > LinkMap ;
  std::vector<double> initRank,newRank;
  int n;
  double alpha;
  template<typename  MapTask>
  class datasource : mapreduce::detail::noncopyable
  {
    public:
      datasource(long size) : sequence_(0),size_(size) 
     {
     }
     bool const setup_key(typename MapTask::key_type &key)
     {
       key = sequence_++; //pages are numbered from 1 to n

       return key < size_;
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
       value = initRank[key];
       return true;
     }
    private:
     unsigned sequence_;
     long const size_;
  };



  struct map_task : public mapreduce::map_task< int, double >
  {
    template<typename Runtime>
    void operator()(Runtime &runtime,key_type const &key , value_type const &value) const
    {

      /*Mapper returns the pageId and corresponding increase 
       in its pageRank because of page whose pageId= key links to it*/
      runtime.emit_intermediate(key,0);
      int nLinks = LinkMap[key].size();
      std::vector<int> LinkVector = LinkMap[key];
      for(int i= 0  ; i < nLinks; i++){
       // std::cout <<  "key " << key << " " << LinkVector[i] << " " << value/nLinks << std::endl;
        runtime.emit_intermediate(LinkVector[i],value/nLinks);
      }
    }
  };                 
  
  struct danglingmap_task : public mapreduce::map_task< int, double >
  {
    template<typename Runtime>
    void operator()(Runtime &runtime,key_type const &key , value_type const &value) const
    {

      int dkey = 0 ;
      runtime.emit_intermediate(dkey,0); //for safety purpose

      int nLinks = LinkMap[key].size();
      if (nLinks == 0)
        runtime.emit_intermediate(dkey,value);
    }
  };                 
  struct reduce_task : public mapreduce::reduce_task<int, double >
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
  >job1;

  typedef 
  mapreduce::job<pageRank::danglingmap_task,
                 pageRank::reduce_task,
                 mapreduce::null_combiner,
                 pageRank::datasource<pageRank::map_task>
  >job2;

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

void writeToFile(std::string filename){
  std::ofstream outfile;
  outfile.open(filename);
  double sum =0 ;
  for(int i = 0 ; i < pageRank::newRank.size() ; i++){
    sum+=pageRank::newRank[i];
    outfile << pageRank::newRank[i] << std::endl;
  }
  outfile << "sum= " << sum << std::endl;
  outfile.close();
}

int main(int argc , char **argv){
  /*taking input from a file in which first 2 integers represent 
   * the number of pages and number of links 
   * and subsequent lines have pair of intgers 
   * representing edge from page1 to page2 */




  std::ifstream inFile ;
  inFile.open(argv[1]);
  int n,links;
  n=0;
  //Pages are numbered from 1 to n and not from 0 to n-1
  int a,b;

  while(inFile >> a >> b ){
      n=std::max(b,std::max(a,n));
      pageRank:: LinkMap[a].push_back(b);
  }
  n++;
  
  pageRank::alpha = .85;

  /*Initilial pageRanks are stored in initRank and newly computed in newRank*/
  pageRank::initRank= std::vector<double>(n,0);
  pageRank::newRank= std::vector<double>(n,1.0/n);

  //Storing the Edges 

  int iteration = 0; //to store the number of count till convergence
  while(!converge(pageRank::initRank,pageRank::newRank,.0001)){
     iteration++;
     pageRank::initRank = pageRank::newRank;
     mapreduce::specification spec;
     pageRank::job1::datasource_type datasource1(n);
     pageRank::job2::datasource_type datasource2(n);
     spec.map_tasks = 1;
     spec.reduce_tasks=std::max(1U,std::thread::hardware_concurrency());

     pageRank::job1 job1(datasource1,spec);
     pageRank::job2 job2(datasource2,spec);

     mapreduce::results result1;
     mapreduce::results result2;
  
   # ifdef _DEBUG 
     job1.run<mapreduce::schedule_policy::sequential<pageRank::job1> >(result1);
   # else 
     job1.run<mapreduce::schedule_policy::cpu_parallel<pageRank::job1> >(result1);
   # endif
     
     for(auto it=job1.begin_results();it!=job1.end_results();++it)
     {
       pageRank::newRank[it->first]=pageRank::alpha*(it->second);
     }
     
   # ifdef _DEBUG 
     job2.run<mapreduce::schedule_policy::sequential<pageRank::job2> >(result2);
   # else 
     job2.run<mapreduce::schedule_policy::cpu_parallel<pageRank::job2> >(result2);
   # endif
   

     for(auto it=job2.begin_results();it!=job2.end_results();++it)
     {  
        for(int i = 0 ; i < n ; i ++){
          pageRank::newRank[i]+= (pageRank::alpha*(it->second) + 1-pageRank::alpha)/n ;
        }
  
     }

  }
  std::cout << "Number of iterations " << iteration << std::endl;
  
  for(int i = 0 ; i < n ; i++){
    std::cout << "Page " <<  i+1 << " " <<  pageRank::newRank[i] << std::endl;
  }
  writeToFile(argv[2]); 
}

