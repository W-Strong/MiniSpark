#define _GNU_SOURCE

#include "minispark.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sched.h>
#include <dirent.h>

ThreadPool* thread_pool;

FILE* log_fp;

int metric_FLAGGGGGG = 0;

TaskMetricNode* head; 
TaskMetricNode* tail; 
pthread_t metric_thread;
pthread_mutex_t metric_queue_lock;

int counter = 0;

int num_threads_at_start = 0;

// Working with metrics...
// Recording the current time in a `struct timespec`:
//    clock_gettime(CLOCK_MONOTONIC, &metric->created);
// Getting the elapsed time in microseconds between two timespecs:
//    duration = TIME_DIFF_MICROS(metric->created, metric->scheduled);
// Use `print_formatted_metric(...)` to write a metric to the logfile. 
void print_formatted_metric(TaskMetric* metric, FILE* fp) {
  fprintf(fp, "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled %10jd.%06ld, execution (usec) %ld\n",
	  metric->rdd, metric->pnum, metric->rdd->trans,
	  metric->created.tv_sec, metric->created.tv_nsec / 1000,
	  metric->scheduled.tv_sec, metric->scheduled.tv_nsec / 1000,
	  metric->duration);
}

int max(int a, int b)
{
  return a > b ? a : b;
}

RDD *create_rdd(int numdeps, Transform t, void *fn, ...)
{
  RDD *rdd = malloc(sizeof(RDD));
  if (rdd == NULL)
  {
    printf("error mallocing new rdd\n");
    exit(4);
  }

  va_list args;
  va_start(args, fn);

  int maxpartitions = 0;
  for (int i = 0; i < numdeps; i++)
  {
    RDD *dep = va_arg(args, RDD *);
    dep->return_rdd = rdd;                  //added this line to intstantiate the return rdd of the dependencies
    rdd->dependencies[i] = dep;
    maxpartitions = max(maxpartitions, dep->partitions->size); // maxpartitions not used?
  }
  va_end(args);

  rdd->numdependencies = numdeps;
  rdd->trans = t;
  rdd->fn = fn;
  rdd->partitions = list_init(maxpartitions);
  rdd->nummaterialized = 0;
  rdd->numpartitions = maxpartitions; 
  pthread_mutex_init(&(rdd->mutex), NULL);
  return rdd;
}

/* RDD constructors */
RDD *map(RDD *dep, Mapper fn)
{
  return create_rdd(1, MAP, fn, dep);
}

RDD *filter(RDD *dep, Filter fn, void *ctx)
{
  RDD *rdd = create_rdd(1, FILTER, fn, dep);
  rdd->ctx = ctx;
  return rdd;
}

RDD *partitionBy(RDD *dep, Partitioner fn, int numpartitions, void *ctx)
{
  RDD *rdd = create_rdd(1, PARTITIONBY, fn, dep);
  rdd->partitions = list_init(numpartitions); ////
  rdd->numpartitions = numpartitions;
  rdd->ctx = ctx;
  return rdd;
}

/**
 * TODO: idk what but it's supposed to be pretty rough (._. ) DONE
 */
RDD *join(RDD *dep1, RDD *dep2, Joiner fn, void *ctx)
{
  RDD *rdd = create_rdd(2, JOIN, fn, dep1, dep2);
  rdd->ctx = ctx;
  return rdd;
}

/* A special mapper */
void *identity(void *arg)
{
  return arg;
}

/* Special RDD constructor.
 * By convention, this is how we read from input files. */
RDD *RDDFromFiles(char **filenames, int numfiles)
{
  RDD *rdd = malloc(sizeof(RDD));
  rdd->partitions = list_init(numfiles);
  pthread_mutex_init(&(rdd->mutex), NULL);//this needs to be before list_get_partition

  for (int i = 0; i < numfiles; i++)
  {
    FILE *fp = fopen(filenames[i], "r");
    if (fp == NULL) {
      perror("fopen");
      exit(5);
    }
    //list_add_elem(rdd->partitions, fp); this is base code
    list_get_partition(rdd, i)->elem = fp;
  }

  rdd->numdependencies = 0;
  rdd->trans = FILE_BACKED;
  rdd->fn = (void *)identity;
  rdd->numpartitions = numfiles;     //added this line
  rdd->nummaterialized = 0;
  
  return rdd;
}

/**
 * Search the DAG recursively to find RDDs with no dependencies (leaf-nodes)
 * When a leaf-node is encountered, push it to the Work Queue
 * 
 * this should be working?
 */
void execute(RDD* rdd) {

  //printf("execute called\n");

  // Base Case:
  if(rdd->numdependencies == 0){    
    //printf("  adding RDD to WQ\n");
    for(int i = 0; i < rdd->numpartitions; i++){
      //printf("  adding task:  part=%d\n",i);
      Task* task = malloc(sizeof(Task));
      task->rdd = rdd;
      task->pnum = i;
      thread_pool_submit(task);   //TODO figure out TaskMetric stuff
    }
    return;
  } 
  
  // Check numdependencies
  if(rdd->numdependencies == 1){    // One dependency
    execute(rdd->dependencies[0]);
  } else if(rdd->numdependencies == 2){ // Two dependencies
    execute(rdd->dependencies[0]);
    execute(rdd->dependencies[1]);
  } 

  return;
}

void MS_Run() {

  cpu_set_t set;
  CPU_ZERO(&set);

  if (sched_getaffinity(0, sizeof(set), &set) == -1) {
    perror("sched_getaffinity");
    exit(6);
  }

  //printf("number of cores available: %d\n", CPU_COUNT(&set));

  int numthreads = CPU_COUNT(&set);
  //numthreads = 1;

  // create a thread pool
  thread_pool_init(numthreads);

  return;
}

void MS_TearDown() {

  metric_FLAGGGGGG = 1;

  thread_pool_destroy();


  

  //for(int i = 0; i < 100000; i++);
  // TODO: destroy RDDs

  return;
}

int count(RDD *rdd) {
  execute(rdd);

  thread_pool_wait();

  Node* partition;
  List* elements;


  int count = 0;
  // count all the items in rdd
  for(int i = 0; i < rdd->numpartitions; i++){
    partition = list_get_partition(rdd, i);
    elements = partition->elements;

    list_seek_to_start(elements);
    for(int i = 0; i < elements->size; i++){
      count++;
      list_next(elements);
    }
  }

  return count;
}

void print(RDD *rdd, Printer p) {

  // remove?
  execute(rdd);

  // wait for rdds to finish their work
  thread_pool_wait();

  Node* partition;
  List* elements;


  // print
  for(int i = 0; i < rdd->numpartitions; i++){
    partition = list_get_partition(rdd, i);
    elements = partition->elements;

    list_seek_to_start(elements);
    for(int i = 0; i < elements->size; i++){
      p(elements->iter->node->elem);
      //fflush(stdout);
      list_next(elements);
    }
    
    // while(elements->iter->node->elem != NULL){
    //   p(elements->iter->node->elem);                      // TODO PROBLEM?!?
    //   if (list_next(elements)){
    //     break;
    //   }
    // }
  }

  // print all the items in rdd
  // aka... `p(item)` for all items in rdd
}


/////////////// list functions /////////////// 

/**
 * TODO:
 */
void list_add_elem(List* list, void* element) {
  // pthread_mutex_lock(&(list->List_mutex));
  list_append(list, element);
  // pthread_mutex_unlock(&(list->List_mutex));
}


/**
 * Initialize the partitions as a linked list
 * returns: ptr to a doubly linked list
 */
List* list_init(int numpartitions) {

  if (numpartitions <= 0) {
    perror("list_init: numpartitions is <= 0");
    return NULL;
  }
  
  List* partitions = malloc(sizeof(List));

  // initialize space for the List
  partitions->head = malloc(sizeof(Node));
  partitions->head->elements = list_init_elements();
  partitions->tail = partitions->head;  // when 1 Node, head is also tail
  partitions->head->pnum = 0;
  partitions->size = numpartitions;
  partitions->iter = malloc(sizeof(Iter));  // create the iterator, assign it first element
  partitions->iter->node = partitions->head;
  pthread_mutex_init(&(partitions->List_mutex), NULL);

  // initialize space for the remainder of the list
  for (int i=1; i<numpartitions; i++) {
    Node* new_node = malloc(sizeof(Node));
    new_node->elements = list_init_elements();
    new_node->pnum = i;
    new_node->prev = partitions->tail;
    partitions->tail->next = new_node;
    partitions->tail = new_node;  // update tail to the new node
  }

  return partitions; // return list of partitions
}

/**
 * initializes the list with an unknown number of nodes (the element list)
 */
List* list_init_elements(){
  List* newList = malloc(sizeof(List));
  newList->iter = (Iter*)malloc(sizeof(Iter));
  newList->iter->node = NULL;
  newList->head = NULL;
  newList->tail = NULL;
  newList->size = 0;
  pthread_mutex_init(&(newList->List_mutex), NULL);
  return newList;
}


/**
 * adds a node with the new element at the back of the list
 */
void list_append(List* list, void* elem) {
  Node* newNode = (Node*)malloc(sizeof(Node));
  newNode->elem = elem;
  if(list->head == NULL){
    list->head = newNode;
    list->tail = newNode;
    list->iter->node = newNode;
    list->size++;
  } else {
    list->tail->next = newNode;
    newNode->prev = list->tail->next;
    newNode->next = NULL;
    list->tail = newNode;
    list->size++;
  }
}

/**
 * Get the partition specified by pnum
 * return NULL if unsuccessful
 * Q: is this supposed to be like next? A: No, returns the partition specified by pnum
 */
Node* list_get_partition(RDD* rdd, int pnum) {   
  
  pthread_mutex_lock(&(rdd->mutex));

  if(pnum < 0){
    pthread_mutex_unlock(&(rdd->mutex));
    return NULL;
  }

  if (list_seek_to_start(rdd->partitions)) {
    pthread_mutex_unlock(&(rdd->mutex));
    printf("\niterator error\n");
    fflush(stdout);
    
    //perror("iterator error");
    return NULL;
  }

  for (int i=0; i<pnum; i++) {
    if (list_next(rdd->partitions)) {
      pthread_mutex_unlock(&(rdd->mutex));
      printf("\niterator error2\n");
      fflush(stdout);
      //perror("iterator error");
      return NULL;
    }
  }
  pthread_mutex_lock(&(rdd->partitions->List_mutex));
  Node* tempNode = rdd->partitions->iter->node;
  pthread_mutex_unlock(&(rdd->partitions->List_mutex));
  pthread_mutex_unlock(&(rdd->mutex));

  
  return tempNode;  // returns, but can it be out of date?
}

/**
 * Frees the memory used to store the partitions
 */
void list_free(List* partitions) {
  // Free partitions
  Node* temp_node;
  Node* curr_node = partitions->head;
  for (int i=0; i<partitions->size; i++) {
    temp_node = curr_node->next;
    free(curr_node);
    curr_node = temp_node;
    if (curr_node == NULL) break;
  }

  // Free iterator
  free(partitions->iter);
  // Free partition list
  free(partitions); 
}

/**
 * Move the iterator to the next partition
 * returns 0 if successful
 */
int list_next(List* list) {
  pthread_mutex_lock(&(list->List_mutex));
  if (!list->iter->node->next) {  // check if end
    pthread_mutex_unlock(&(list->List_mutex));
    return 1;
  }
  list->iter->node = list->iter->node->next;
  list->iter->curr_pos += 1;
  pthread_mutex_unlock(&(list->List_mutex));

  return 0;
}

/**
 * Move the iterator to the first partition
 * returns 0 if successful
 */
int list_seek_to_start(List* list) {
  pthread_mutex_lock(&(list->List_mutex));
  if (list == NULL) {
    pthread_mutex_unlock(&(list->List_mutex));
    return 1;
  }
  if (list->head) {
    list->iter->node = list->head;
    pthread_mutex_unlock(&(list->List_mutex));
    return 0;
  }
  pthread_mutex_unlock(&(list->List_mutex));
  return 1;
}



/////////////// thread pool functions ////////////////



/**
 * Waits for work to be assigned, and then does that work
 * number of calls depends on number of threads created
 * TODO: add locks for safety             DONE
 * TODO: implementation for doing work    DONE
 */
void* worker(void* p) {
  Task* task;
  // int counter = 0;
  while (1) {
    //first get a thing from the work queue, it will wait until it can pull something off of the work queue
    task = work_queue_pop(thread_pool->work_queue);

    if(thread_pool->tp_alive == 0){
      return p;
    }
    // printf("thread: %x  pop\n", pthread_self());
    fflush(stdout);
    
    // printf("poped rdd: %p of type:%d to func: %p with pnum: %d\n", task->rdd, task->rdd->trans, task->rdd->fn, task->pnum);
    // counter++;
    // printf("\n  thread got a task : %d task_pointer: %p\n", counter, task);
    fflush(stdout);

    

    //update the threadpool info
    

    //now task has the task that needs to be completed, so do the work now

    switch(task->rdd->trans) {
      case MAP:
        do_map(task);
        break;
      case FILTER:
        do_filter(task);
        break;
      case JOIN:
        do_join(task);
        break;
      case PARTITIONBY:
        do_partition(task);
        break;
      case FILE_BACKED:
        break;
    }

    // once the work is done, update the RDD num materialized, if it is now equal to num partitions, '
    // add the backlink to the work_queue

    pthread_mutex_lock(&(task->rdd->mutex));
    task->rdd->nummaterialized++;
    
    int is_final_rdd = 0;


    // for(int i = 0; i < 10000000; i++);
    //printf("    size of WQ: %d\n", thread_pool->work_queue->size);
    //printf("nummat: %d    nump: %d\n", task->rdd->nummaterialized, task->rdd->numpartitions);

    // printf("TASK: %p, nummat = %d, numpart = %d\n", task, task->rdd->nummaterialized, task->rdd->numpartitions);
    fflush(stdout);
    if(task->rdd->nummaterialized == task->rdd->numpartitions){
      // printf("checking to see if task: %p made it here\n", task);
      // fflush(stdout);
      pthread_mutex_unlock(&(task->rdd->mutex));
      // add tasks if next rdd exists
      if (task->rdd->return_rdd != NULL) { 

        pthread_mutex_lock(&(task->rdd->return_rdd->mutex));
        task->rdd->return_rdd->numdependencies--;
        
        
        // indicate to next rdd to begin work
        if(task->rdd->return_rdd->numdependencies == 0){
          pthread_mutex_unlock(&(task->rdd->return_rdd->mutex));
          // add new partitions to the work queue
          // printf("thread: %x  adding to WQ\n", pthread_self());
          fflush(stdout);
          for(int i = 0; i < task->rdd->return_rdd->numpartitions; i++){
            Task* newtask = malloc(sizeof(Task));
            newtask->rdd = task->rdd->return_rdd;
            newtask->pnum = i;

            // printf("pushed rdd: %p of type:%d to func: %p with pnum: %d\n", newtask->rdd, newtask->rdd->trans, newtask->rdd->fn, newtask->pnum);
            fflush(stdout);

            thread_pool_submit(newtask);   //TODO figure out TaskMetric stuff
          }
        } else {
          pthread_mutex_unlock(&(task->rdd->return_rdd->mutex));
        }

        
      } else {
        // check if this is the last task ie returnrdd = null 
        // printf("\nIS FINAL\n");
        fflush(stdout);
        is_final_rdd = 1;
      }
    } else {
      // printf("thread: %d nummat != numpart\n", pthread_self());
      fflush(stdout);
      pthread_mutex_unlock(&(task->rdd->mutex));
    }

    

    //FREE THE TASK
    //printf("    TASK POINTER: %p\n      RDD->trans: %d\n", task, task->rdd->trans);
    //fflush(stdout);
    free(task);
    

    //update the threadpool info
    pthread_mutex_lock(&(thread_pool->TP_lock));
    thread_pool->num_working--;
    pthread_mutex_unlock(&(thread_pool->TP_lock));

    if (is_final_rdd) {
      // move to end TODO
      pthread_cond_signal(&(thread_pool->task_done));
      //printf("\n  thread complete, self-destructing\n");
      //return p;
    }
  }
  
  return p; // ????
};

///////////////// WORKER HELPERS ///////////////// 

/**
 * Does the work for the MAP transformation
 * Mapping utilizes one-to-one mapping between partitions
 */
int do_map(Task* task) {

  // printf("\n\n\nmade it to do_map\n\n\n");
  // fflush(stdout);
  
  Node* source_partition = list_get_partition(task->rdd->dependencies[0], task->pnum); // Source partition
  Node* dest_partition = list_get_partition(task->rdd, task->pnum); // destination partititon
  
  List* source_elements = source_partition->elements;
  List* dest_elements = dest_partition->elements;
  
  void* new_elem;
  void* old_elem;
 
  if(task->rdd->dependencies[0]->trans == FILE_BACKED){ // this means that we are doing getlines
    old_elem = strdup(source_partition->elem);
    while(1){

      new_elem = ((Mapper)task->rdd->fn)(source_partition->elem); // do work via function pointer
      if(new_elem == NULL){
        break;
      } else {
        list_add_elem(dest_elements, new_elem);
      }
    };
    return 0;
  } else { //this means that we are doing splitcols
    list_seek_to_start(source_elements);

    void* old_elem;

    if(source_elements->size == 0){ //if there are no elements in the partition
      return 0;
    }

    for(int i = 0; i < source_elements->size; i++){
      old_elem = source_elements->iter->node->elem;
      new_elem = ((Mapper)task->rdd->fn)(old_elem);
      list_add_elem(dest_elements, new_elem);
      list_next(source_elements);
    }
  }

  return 0;
}

/**
 * Does the work for the FILTER transform
 * Filtering utilizes one-to-one mapping between partitions?
 */
int do_filter(Task* task) {
  Node* source_partition = list_get_partition(task->rdd->dependencies[0], task->pnum); // Source partition
  Node* dest_partition = list_get_partition(task->rdd, task->pnum); // destination partititon
  
  List* source_elements = source_partition->elements;
  List* dest_elements = dest_partition->elements;

  void* elem;

  int status;

  list_seek_to_start(source_elements);

  if(source_elements->size == 0){ //if there are no elements in the partition
    return 0;
  }

  for(int i = 0; i < source_elements->size; i++){
    elem = source_elements->iter->node->elem;
    status = ((Filter)task->rdd->fn)(elem, task->rdd->ctx); // call 'fn'
    if(status == 1){
      //printf("\n\n\n            ELEM*: %p   pnum: %d\n\n\n", elem, task->pnum);
      //fflush(stdout);
      list_add_elem(dest_elements, elem);
    }
    list_next(source_elements);
  }

  return 0;
}

/**
 * Does the work for the JOIN transform
 * Multiple source rdds map to one destination rdd
 * Partitions are one-to-one mapping
 */
int do_join(Task* task){
  
  Node* source_partition_1 = list_get_partition(task->rdd->dependencies[0], task->pnum); // Source partition
  Node* source_partition_2 = list_get_partition(task->rdd->dependencies[1], task->pnum);
  Node* dest_partition = list_get_partition(task->rdd, task->pnum); // destination partititon

  List* source_elements_1 = source_partition_1->elements;
  List* source_elements_2 = source_partition_2->elements;
  List* dest_elements = dest_partition->elements;

  void* old_elem_1;
  void* old_elem_2;
  void* new_elem;

  list_seek_to_start(source_elements_1);
  list_seek_to_start(source_elements_2);
  
  for(int i = 0; i < source_elements_1->size; i++){
    old_elem_1 = source_elements_1->iter->node->elem;
    list_seek_to_start(source_elements_2);
    for(int j = 0; j < source_elements_2->size; j++){
      old_elem_2 = source_elements_2->iter->node->elem;
      //printf("\n\n\n        source1: %d     source2: %d\n\n\n", i, j);
      //fflush(stdout);
      new_elem = ((Joiner)task->rdd->fn)(old_elem_1, old_elem_2, task->rdd->ctx);
      if(new_elem != NULL){
        list_add_elem(dest_elements, new_elem);
      }
      list_next(source_elements_2);
    }
    list_next(source_elements_1);
  }

  // while((old_elem_1 = source_elements_1->iter->node->elem) != NULL){
  //   while((old_elem_2 = source_elements_2->iter->node->elem) != NULL){
  //     new_elem = ((Joiner)task->rdd->fn)(old_elem_1, old_elem_2, task->rdd->ctx);
  //     if(new_elem != NULL){
  //       list_add_elem(dest_elements, new_elem);
  //     }
  //     if(list_next(source_elements_2)){
  //       break;
  //     }
  //   }
  //   if(list_next(source_elements_1)){
  //     break;
  //   }
  // }

  return 0;
}


/**
 * Does the work for the PARTITIONER transform
 * Multiple source partitions map to different 
 */
int do_partition(Task* task){

  //so we have a source partition that we get the destination partition numbers of by
  //calling the function, then we add it to that partition in the rdd
  
  Node* dest_partition = list_get_partition(task->rdd, task->pnum);
  List* dest_elements = dest_partition->elements;

  void* elem;
  list_seek_to_start(dest_elements);

  Node* source_partition;
  List* source_elements;
  long pnum;

  for(int i = 0; i < task->rdd->dependencies[0]->numpartitions; i++){
    source_partition = list_get_partition(task->rdd->dependencies[0], i);
    source_elements = source_partition->elements;
    //lock the source partitions elements while reading through it to keep the iterator at the correct node
    pthread_mutex_lock(&(source_elements->List_mutex));
    //list_seek_to_start(source_elements); can't use this because we need to have the partition locked the whole time, itll get stuck if it tries to lock twice
    
    if (source_elements->head) { //if there are elements in the list put the iterator at the start, otherwise skip to the next partition
      source_elements->iter->node = source_elements->head;
    } else {
      continue;
    }

    for(int j = 0; j < source_elements->size; j++){
      elem = source_elements->iter->node->elem;
      pnum = ((Partitioner)task->rdd->fn)(elem, task->rdd->numpartitions, task->rdd->ctx);
      if(pnum == task->pnum){
        list_add_elem(dest_elements, elem);
      }
      //list_next(source_elements); same thing as above, cant use it because we need the list to be locked the entire time we are reading it
      source_elements->iter->node = source_elements->iter->node->next;
      //source_elements->iter->curr_pos += 1;

    }
    pthread_mutex_unlock(&(source_elements->List_mutex));
  }

  return 0;
  
}

///////////////////////thread pool functions////////////////////////////


 /**
  * Create the thread pool with numthreads threads. Do any necessary allocations. 
  * Threads will sleep until work is assigned via work_queue, at which point 
  * one (or more) will wake and consume that task. 
  * TODO:
  */
 void thread_pool_init(int numthreads) {

  
    
  
  if(numthreads < 0){
    numthreads = 0;
  }

  thread_pool = malloc(sizeof(ThreadPool));

  if(thread_pool == NULL){
    perror("thread_pool _init : could not allocate ThreadPool struct");
    exit(7);
  }

  // Initialize the work queue
  if(work_queue_init() == -1){
    perror("thread_pool_init : work_queue_init failed");
    free(thread_pool);
    exit(2);
  }

  // Initialize the threads
  thread_pool->threads = malloc(numthreads * sizeof(pthread_t));
  thread_pool->numthreads = numthreads;

  if(thread_pool->threads == NULL){
    perror("thread_pool_init : thread ptrs failed to allocate");
    exit(3);
  }

  thread_pool->tp_alive = 1;

  pthread_mutex_init(&thread_pool->TP_lock, NULL);

  pthread_cond_init(&(thread_pool->task_done), NULL);

  task_metric_init();

  for (int i=0; i<numthreads; i++) {
    pthread_create(&thread_pool->threads[i], NULL, worker, NULL); 
  }

  

  int count = 0;
  DIR *dir = opendir("/proc/self/task");
  if (!dir) {
      perror("opendir");
      exit(1);
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
      // Skip the special entries "." and ".."
      if (entry->d_name[0] == '.')
          continue;
      count++;
  }
  closedir(dir);

  num_threads_at_start = count - numthreads - 1;
  //return thread_pool; // return ptr to the thread pool
  
}

/**
 * join all the threads and deallocate any memory used by the pool. 
 * TODO:
*/
void thread_pool_destroy(){
  thread_pool_wait();

  pthread_mutex_lock(&thread_pool->TP_lock);
  thread_pool->tp_alive = 0;
  pthread_mutex_unlock(&thread_pool->TP_lock);
  
  for(int i = 0; i < thread_pool->numthreads; i++){
    sem_post(thread_pool->work_queue->full);
  }
  for(int i = 0; i < thread_pool->numthreads; i++){
    pthread_join(thread_pool->threads[i], NULL);
  }

  work_queue_destroy(thread_pool->work_queue);

  task_metric_destroy();

  pthread_mutex_destroy(&(thread_pool->TP_lock));
  free(thread_pool->threads);

  int count;
  while(1){
    count = 0;
    DIR *dir = opendir("/proc/self/task");
    if (!dir) {
        perror("opendir");
        exit(1);
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        // Skip the special entries "." and ".."
        if (entry->d_name[0] == '.')
            continue;
        count++;
    }
    closedir(dir);
    if(count == num_threads_at_start){
      break;
    }
  }
}

/**
 * Returns when the work queue is empty and all threads have finished 
 * their tasks. You can use it to wait until all the tasks in the queue 
 * are finished. For example, you would not want to count before RDD is 
 * fully materialized.
 * TODO:
 */
void thread_pool_wait(){
  // for (int i=0; i<1000000; i++) {int x; x += i; }
  //printf("");
  // pthread_cond_wait(&(thread_pool->task_done), &(thread_pool->task_done_lock)); // TODO: ask TA about CV and Locks 

  pthread_mutex_lock(&(thread_pool->work_queue->WQ_mutex));
  pthread_mutex_lock(&(thread_pool->TP_lock));
  while (thread_pool->num_working != 0 || thread_pool->work_queue->size != 0){
    pthread_mutex_unlock(&(thread_pool->work_queue->WQ_mutex));
    pthread_cond_wait(&(thread_pool->task_done), &(thread_pool->TP_lock)); // TODO: ask TA about CV and Locks 
    // pthread_mutex_lock(&(thread_pool->work_queue->WQ_mutex));
  }
  pthread_mutex_unlock(&(thread_pool->TP_lock));
  // pthread_mutex_unlock(&(thread_pool->work_queue->WQ_mutex));
  // printf("done with materialize\n");
  return;
}

/**
 * Adds a task to the work queue
 * Function should producer_sleep if work queue is full (256 tasks) to 
 * wait for the queue to empty 
 */
void thread_pool_submit(Task* task){

  // create Task Metric
  task_metric_task_create(task);

  // add work to queue
  work_queue_push(thread_pool->work_queue, task);
  
}

/////////////////////////WORK QUEUE CODE//////////////////////////////

/**
 * Gets passed a pointer to a work queue that need to be initialized
 */
int work_queue_init(){

  WQueue* work_queue = malloc(sizeof(WQueue));
  thread_pool->work_queue = work_queue;
  work_queue->size = 0;
  work_queue->head = NULL;
  work_queue->tail = NULL;

  work_queue->empty = (struct Sem*) malloc(sizeof(Sem));
  if(work_queue == NULL){
    return -1;
  }

  work_queue->full = (struct Sem*) malloc(sizeof(Sem));
  if(work_queue == NULL){
    return -1;
  }

  pthread_mutex_init(&(work_queue->WQ_mutex), NULL);
  sem_init(work_queue->empty, MAXWORKQUEUESIZE);
  sem_init(work_queue->full, 0);
  return 0;
}

/**
 * @brief Adds a task to the work queue
 * 
 * @param work_queue 
 * @param task 
 */
void work_queue_push(WQueue* work_queue, Task* task){

  // printf("thread: %x  waiting\n", pthread_self());
  fflush(stdout);
  sem_wait(work_queue->empty);  //wait for the queue to have room
  // printf("thread: %x  no longer waiting\n", pthread_self());
  fflush(stdout);

  pthread_mutex_lock(&(work_queue->WQ_mutex));
  task->next = NULL;

  // printf("\npush    size: %d   task type: %d  rdd: %p with pnum: %d  \n", work_queue->size, task->rdd->trans, task->rdd, task->pnum);
  // printf("\npush to WQ:   %d\n", ++counter);
  fflush(stdout);

  if(work_queue->size == 0){
    work_queue->head = task;
    work_queue->tail = task;
  } else {
    work_queue->tail->next = task;
    work_queue->tail = task;
  }
  
  work_queue->size++;
  
  // printf("thread: %x  sempost WQ->full    WQ: %d\n", pthread_self(), thread_pool->work_queue->size);
  fflush(stdout);

  sem_post(work_queue->full);
  pthread_mutex_unlock(&(work_queue->WQ_mutex));
}

/**
 * @brief get a task from the head of the work queue
 * 
 * @param work_queue 
 * @param task 
 */
Task* work_queue_pop(WQueue* work_queue){

  sem_wait(work_queue->full);  //wait for the queue to have a task

  // printf("thread: %x  semwait no longer waiting\n", pthread_self());
  fflush(stdout);

  pthread_mutex_lock(&(thread_pool->TP_lock));
  if(thread_pool->tp_alive == 0){
    pthread_mutex_unlock(&(thread_pool->TP_lock));
    return NULL;
  }
  pthread_mutex_unlock(&(thread_pool->TP_lock));

  Task* task;

  pthread_mutex_lock(&(thread_pool->TP_lock));
  thread_pool->num_working++;
  pthread_mutex_unlock(&(thread_pool->TP_lock));

  pthread_mutex_lock(&(work_queue->WQ_mutex));

  // printf("pop from WQ:  %d\n", --counter);
  fflush(stdout);

  task = work_queue->head;


  if(work_queue->size == 1){
    work_queue->head = NULL;
    work_queue->tail = NULL;
  } else {
    work_queue->head = task->next;
  }

  work_queue->size--;

  // printf("pop     size: %d   task type: %d  rdd: %p with pnum: %d\n", work_queue->size, task->rdd->trans, task->rdd, task->pnum);
  fflush(stdout);

  // printf("thread: %x  sempost WQ->empty   WQ: %d\n", pthread_self(), thread_pool->work_queue->size);
  fflush(stdout);

  sem_post(work_queue->empty);

  pthread_mutex_unlock(&(work_queue->WQ_mutex));


  // add task to metrics
  task_metric_task_fetched(task);
  task_metric_queue_add(task);

  return task;
}


void work_queue_destroy(WQueue* work_queue){
  pthread_mutex_unlock(&(work_queue->WQ_mutex));
  sem_destroy(work_queue->empty);
  sem_destroy(work_queue->full);
  pthread_mutex_destroy(&(work_queue->WQ_mutex)); // TODO: "destroy of a locked mutex", likely in thread_pool_wait
}


/////////////////////////SEMAPHORE CODE//////////////////////////////

void sem_init(Sem* sem, int init_num){
  pthread_mutex_init(&(sem->Sem_mutex), NULL);
  pthread_cond_init(&(sem->Sem_cond), NULL);
  sem->v = init_num;
}

void sem_post(Sem* sem){
  pthread_mutex_lock(&(sem->Sem_mutex));
  sem->v++;
  pthread_cond_signal(&(sem->Sem_cond));
  pthread_mutex_unlock(&(sem->Sem_mutex));
}

void sem_wait(Sem* sem){
  pthread_mutex_lock(&(sem->Sem_mutex));
	while (sem->v <= 0) {
		pthread_cond_wait(&(sem->Sem_cond), &(sem->Sem_mutex));
	}
	sem->v--;
	pthread_mutex_unlock(&(sem->Sem_mutex));
}

void sem_destroy(Sem* sem){
  pthread_mutex_destroy(&(sem->Sem_mutex));
  pthread_cond_destroy(&(sem->Sem_cond));
}

///////////////////////// TASK METRIC ///////////////////////// 

void* metrics(void* p) {
  TaskMetricNode* node_to_print = NULL;
  while (1){

    // check if main threads are done
    pthread_mutex_lock(&thread_pool->TP_lock);
    if(thread_pool->tp_alive == 0){
      pthread_mutex_unlock(&thread_pool->TP_lock);
      return p;
    }
    pthread_mutex_unlock(&thread_pool->TP_lock);

    pthread_mutex_lock(&metric_queue_lock);
    if(head == NULL){
      pthread_mutex_unlock(&metric_queue_lock);
      continue;
    } else {
      node_to_print = head;

      // do the print 
      print_formatted_metric(node_to_print->task_metric, log_fp);
      // replace the head and free prev head 
      head = head->next;
      if (head == NULL) {
        tail = NULL;
      }
      free(node_to_print->task_metric);
      free(node_to_print);
    }

    pthread_mutex_unlock(&metric_queue_lock);
    
  }
}

void task_metric_init() {
  log_fp = fopen("metrics.log", "w");
  pthread_mutex_init(&metric_queue_lock, NULL);
  pthread_create(&metric_thread, NULL, metrics, NULL);
  
}


/**
 * 
 */
void task_metric_task_create(Task* task) {
  task->metric = malloc(sizeof(TaskMetric));
  task->metric->rdd = task->rdd;
  task->metric->pnum = task->pnum;
  clock_gettime(CLOCK_MONOTONIC, &task->metric->created);
}

/**
 * 
 */
void task_metric_task_fetched(Task* task) {
  clock_gettime(CLOCK_MONOTONIC, &task->metric->scheduled);
  task->metric->duration = TIME_DIFF_MICROS(task->metric->created, task->metric->scheduled);
}

/**
 * 
 */
void task_metric_queue_add(Task* task){
  pthread_mutex_lock(&metric_queue_lock);
  TaskMetricNode* new_metric = malloc(sizeof(TaskMetricNode));
  new_metric->task_metric = task->metric;
  new_metric->next = NULL;
  if(head == NULL){
    head = new_metric;
    tail = new_metric;
  } else {
    tail->next = new_metric;
    tail = new_metric;
  }
  pthread_mutex_unlock(&metric_queue_lock);
}

/**
 * 
 */
void task_metric_destroy() {

  pthread_join(metric_thread, NULL);
  pthread_mutex_destroy(&metric_queue_lock);
}