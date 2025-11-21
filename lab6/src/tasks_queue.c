#include <stdio.h>
#include <stdlib.h>

#include "tasks_queue.h"
#include <pthread.h>
tasks_queue_t* create_tasks_queue(void)
{
    tasks_queue_t *q = (tasks_queue_t*) malloc(sizeof(tasks_queue_t));

    q->task_buf_size = QUEUE_CAPACITY;
    q->task_buffer = (task_t**) malloc(sizeof(task_t*) * q->task_buf_size);

    q->index = 0;

    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->not_empty, NULL);

    return q;
}


void free_tasks_queue(tasks_queue_t *q)
{
    /* IMPORTANT: We chose not to free the queues to simplify the
     * termination of the program (and make debugging less complex) */
    
    /* free(q->task_buffer); */
    /* free(q); */
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->not_empty);
}


void enqueue_task(tasks_queue_t *q, task_t *t)
{
    if(q->index == q->task_buf_size){
        fprintf(stderr,"ERROR: the queue of tasks is full\n");
        exit(EXIT_FAILURE);
    }

    q->task_buffer[q->index] = t;
    q->index++;
}


task_t* dequeue_task(tasks_queue_t *q)
{
    if(q->index == 0){
        return NULL;
    }

    task_t *t = q->task_buffer[q->index-1];
    q->index--;

    return t;
}

