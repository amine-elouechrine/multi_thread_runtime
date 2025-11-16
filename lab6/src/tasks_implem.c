#include <stdio.h>
#include "tasks_implem.h"
#include "debug.h"
#include <stdlib.h>
tasks_queue_t *tqueue = NULL;
pthread_t *workers;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t full = PTHREAD_COND_INITIALIZER;
int submitted_task_count = 0;
int completed_task_count = 0;
pthread_cond_t all_tasks_done = PTHREAD_COND_INITIALIZER;
int turn_off = 0;
void create_queues(void)
{
    tqueue = create_tasks_queue();
}

void delete_queues(void)
{
    free_tasks_queue(tqueue);
}    

void *worker_func(void *arg){//consumer
    PRINT_DEBUG(10, "Worker thread started.\n");
    task_t *new_task;
    while(1){
        pthread_mutex_lock(&mutex);
            while(tqueue->index==0 && turn_off ==0 ){
                pthread_cond_wait(&empty,&mutex);
            }

            //check if we have to be turned off

            if(turn_off){
                pthread_mutex_unlock(&mutex);
                break;
            }
            //new task
            new_task = get_task_to_execute();
        pthread_mutex_unlock(&mutex);


        if(new_task!=NULL){
            active_task = new_task;

            int result = exec_task(new_task);
            if(result == TASK_COMPLETED){
                terminate_task(new_task);
            }else if(result == TASK_TO_BE_RESUMED){
                new_task->status = WAITING;
            }
            active_task = NULL;
        }
    }
    return NULL;

}

void create_thread_pool(void)
{   
    workers = malloc(sizeof(pthread_t)*THREAD_COUNT);
    if(workers == NULL){
        perror("ALLOCATION ERROR");
    }


    for (size_t i = 0; i < THREAD_COUNT; i++)
    {
        /* code */
       if(pthread_create(&workers[i],NULL,worker_func,NULL)!=0){
            perror("THREAD CREATION PROBLEM\n");
       }
        
    }

}

void dispatch_task(task_t *t)//producer
{   
    pthread_mutex_lock(&mutex);
        if(tqueue->index == tqueue->task_buf_size){
            tqueue->task_buf_size=tqueue->task_buf_size*2;
            
            task_t** resized_buffer= realloc(tqueue->task_buffer,sizeof(task_t*)*tqueue->task_buf_size);
            if(resized_buffer==NULL){
                perror("Error while allocating");
                exit(EXIT_FAILURE);            
            }
            tqueue->task_buffer=resized_buffer;
        }   
        //empty slot
       enqueue_task(tqueue, t); 
       pthread_cond_signal(&empty);
    pthread_mutex_unlock(&mutex);
}

task_t* get_task_to_execute(void)
{
    return dequeue_task(tqueue);
}

unsigned int exec_task(task_t *t)
{
    t->step++;
    t->status = RUNNING;

    PRINT_DEBUG(10, "Execution of task %u (step %u)\n", t->task_id, t->step);
    
    unsigned int result = t->fct(t, t->step);
    
    return result;
}

void terminate_task(task_t *t)
{
    t->status = TERMINATED;
    
    PRINT_DEBUG(10, "Task terminated: %u\n", t->task_id);
    task_t *parent_to_wake = NULL;
    pthread_mutex_lock(&mutex);

    completed_task_count++; 

    // check if there's other tasks left to do 
    if (completed_task_count == submitted_task_count) {
        pthread_cond_signal(&all_tasks_done);
    }

    

#ifdef WITH_DEPENDENCIES

    if(t->parent_task != NULL){//it does  have a parent 
        task_t *parent = t->parent_task;
        parent->task_dependency_done++;

        //check if we still have some dependencies 
        if (parent->task_dependency_done == parent->task_dependency_count) {
            parent->status = READY;
            parent_to_wake = parent;
            
        }
    }
#endif
    pthread_mutex_unlock(&mutex);

    if(parent_to_wake!=NULL){
        dispatch_task(parent_to_wake);

    }

}

void task_check_runnable(task_t *t)
{
#ifdef WITH_DEPENDENCIES
    pthread_mutex_lock(&mutex);
        int done=  t->task_dependency_done == t->task_dependency_count;
    pthread_mutex_unlock(&mutex);
    

    if(done){
        t->status = READY;// all the childern are finished 
        dispatch_task(t);
    }
#endif
}
