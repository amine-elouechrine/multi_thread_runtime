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
            pthread_cond_signal(&full);
        pthread_mutex_unlock(&mutex);


        if(new_task!=NULL){
            int result = exec_task(new_task);
            if(result == TASK_COMPLETED){
                terminate_task(new_task);
            }
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

void dispatch_task(task_t *t)
{   
    pthread_mutex_lock(&mutex);
        while(tqueue->index == tqueue->task_buf_size){
            pthread_cond_wait(&full,&mutex);
        }
        //empty slot
       enqueue_task(tqueue, t); 
       submitted_task_count++;
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
    pthread_mutex_lock(&mutex);

    completed_task_count++; 

    // check if there's other tasks left to do 
    if (completed_task_count == submitted_task_count) {
        pthread_cond_signal(&all_tasks_done);
    }

    pthread_mutex_unlock(&mutex);

#ifdef WITH_DEPENDENCIES
    if(t->parent_task != NULL){
        task_t *waiting_task = t->parent_task;
        waiting_task->task_dependency_done++;
        
        task_check_runnable(waiting_task);
    }
#endif

}

void task_check_runnable(task_t *t)
{
#ifdef WITH_DEPENDENCIES
    if(t->task_dependency_done == t->task_dependency_count){
        t->status = READY;
        dispatch_task(t);
    }
#endif
}
