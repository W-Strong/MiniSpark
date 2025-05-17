#ifndef __minispark_h__
#define __minispark_h__

#include <pthread.h>
#include <time.h>
 
#define MAXDEPS (2)
#define TIME_DIFF_MICROS(start, end) \
  (((end.tv_sec - start.tv_sec) * 1000000L) + ((end.tv_nsec - start.tv_nsec) / 1000L))
#define MAXWORKQUEUESIZE (1000000)


struct RDD;
struct List;
struct Node;
struct Iter;
struct WQueue;
struct Task;
struct TaskMetric;

typedef struct RDD RDD; // forward decl. of struct RDD
typedef struct List List; // forward decl. of List.
// Minimally, we assume "list_add_elem(List *l, void*)"

/* We added these struct defs */
typedef struct Node Node;
typedef struct Iter Iter;
typedef struct Sem Sem;
typedef struct WQueue WQueue;
typedef struct ThreadPool ThreadPool;
typedef struct Task Task;
typedef struct TaskMetric TaskMetric;
typedef struct TaskMetricNode TaskMetricNode;

// Different function pointer types used by minispark
typedef void* (*Mapper)(void* arg);
typedef int (*Filter)(void* arg, void* pred);
typedef void* (*Joiner)(void* arg1, void* arg2, void* arg);
typedef unsigned long (*Partitioner)(void *arg, int numpartitions, void* ctx);
typedef void (*Printer)(void* arg);

typedef enum {
  MAP,
  FILTER,
  JOIN,
  PARTITIONBY,
  FILE_BACKED
} Transform;

struct RDD {    
  Transform trans; // transform type, see enum
  void* fn; // transformation function
  void* ctx; // used by minispark lib functions
  List* partitions; // list of partitions
  
  RDD* dependencies[MAXDEPS];
  int numdependencies; // 0, 1, or 2

  // you may want extra data members here
  int numpartitions;  // added to prevent error within minispark.c: partitionBy()
  RDD* return_rdd;    // TODO: add links to return to higher RDD? DONE
  int nummaterialized;  // rithik told us to
  pthread_mutex_t mutex; //lock for threads to update nummaterialized
};

struct Node {   // changed 'List' into a list of Nodes (the partitions)
  Node* next;
  Node* prev;
  int pnum; // partition number (ie. part. ID) only used in list of partitions
  //int offset; //doesnt look like its needed? not used in code
  void* elem;  
  List* elements;
};

struct Iter {
  int curr_pos;
  int desired_pos;  // remove?
  Node* node;
};

struct List {
  pthread_mutex_t List_mutex;
  Node* head;
  Node* tail;
  Iter* iter;
  int size;
}; 

struct TaskMetric{
  struct timespec created;
  struct timespec scheduled;
  size_t duration; // in usec
  RDD* rdd;
  int pnum;
};

struct Task{
  RDD* rdd;
  int pnum; //partition index?
  TaskMetric* metric;
  Task* next;
};

struct TaskMetricNode{
  TaskMetric* task_metric;
  TaskMetricNode* next;
};

//////// thread pool structs //////////

typedef struct Sem {
  pthread_mutex_t Sem_mutex;
  pthread_cond_t Sem_cond;
  int v;
} Sem;

typedef struct WQueue {
  pthread_mutex_t WQ_mutex;
  Task* head;
  Task* tail;
  int size;
  Sem* empty;
  Sem* full;
} WQueue;

typedef struct ThreadPool {
  pthread_t* threads;
  pthread_mutex_t TP_lock;
  int numthreads;
  int num_working;  // number of busy threads
  WQueue* work_queue;
  int tp_alive;
  pthread_cond_t task_done;
} ThreadPool;



//////////// list functions //////////// 
// NOTE: ensure thread safety via locks/mutex

void list_add_elem(List*, void*);   /** TODO: */

/* constructor for the List */
List* list_init(int);

List* list_init_elements();

/* add items to the end of the list */
void list_append(List*, void*);

/* get partition from RDD */
Node* list_get_partition(RDD*, int);

/* free the list from memory */
void list_free(List*);

/* iterate to the next item, by incr. offset */
int list_next(List*);

/* reset the offset to 0 */
int list_seek_to_start(List*);


//////////////worker functions//////////////////////

void* worker(void*);

int do_map(Task*);

int do_filter(Task*);

int do_join(Task*);

int do_partition(Task*);

///////////// thread pool and work queue ///////////////
/*create the pool with
numthreads threads. Do any necessary allocations. */
void thread_pool_init(int);

/*join all the threads and deallocate any
memory used by the pool. */
void thread_pool_destroy();

/* returns when the work queue is empty and all
threads have finished their tasks. You can use it to wait until all
the tasks in the queue are finished. For example, you would not want
to count before RDD is fully materialized.*/
void thread_pool_wait();

/* adds a task to the work queue.*/
void thread_pool_submit(Task*);

/*initializes the work queue*/
int work_queue_init();

void work_queue_push(WQueue*, Task*);

Task* work_queue_pop(WQueue*);

/* */
void work_queue_destroy(WQueue*);

void sem_init(Sem*, int);

void sem_post(Sem*);

void sem_wait(Sem*);

void sem_destroy(Sem*);


//////// actions ////////

// Create an RDD with "rdd" as its dependency and "fn"
// as its transformation.
RDD* map(RDD* rdd, Mapper fn);


// Return the total number of elements in "dataset"
int count(RDD* dataset);

// Print each element in "dataset" using "p".
// For example, p(element) for all elements.
void print(RDD* dataset, Printer p);

//////// trans    execute(rdd->dependencies[0]);sformation. "ctx" should be passed to "fn"
// when it is called as a Filter
RDD* filter(RDD* rdd, Filter fn, void* ctx);

// Create an RDD with two dependencies, "rdd1" and "rdd2"
// "ctx" should be passed to "fn" when it is called as a
// Joiner.
RDD* join(RDD* rdd1, RDD* rdd2, Joiner fn, void* ctx);

// Create an RDD with "rdd" as a dependency. The new RDD
// will have "numpartitions" number of partitions, which
// may be different than its dependency. "ctx" should be
// passed to "fn" when it is called as a Partitioner.
RDD* partitionBy(RDD* rdd, Partitioner fn, int numpartitions, void* ctx);

// Create an RDD which opens a list of files, one per
// partition. The number of partitions in the RDD will be
// equivalent to "numfiles."
RDD* RDDFromFiles(char* filenames[], int numfiles);

//////// MiniSpark ////////
// Submits work to the thread pool to materialize "rdd".
void execute(RDD* rdd);

// Creates the thread pool and monitoring thread.
void MS_Run();

// Waits for work to be complete, destroys the thread pool, and frees
// all RDDs allocated during runtime.
void MS_TearDown();


#endif // __minispark_h__
