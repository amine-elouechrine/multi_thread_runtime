#include <stdio.h>
#include "tasks_implem.h"
#include "debug.h"
#include "tasks.h"
#include <stdlib.h>

/* per-thread queues */
tasks_queue_t **tqueues = NULL;
pthread_t *workers;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t full = PTHREAD_COND_INITIALIZER;
int submitted_task_count = 0;
int completed_task_count = 0;
pthread_cond_t all_tasks_done = PTHREAD_COND_INITIALIZER;
int turn_off = 0;
int rr_index = 0;//Round Robin index
int *thread_ids = NULL;

void create_queues(void)
{
    tqueues = malloc(sizeof(tasks_queue_t *) * THREAD_COUNT);
    if (tqueues == NULL)
    {
        perror("create_queues: allocation failed");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < THREAD_COUNT; i++)
    {
        tqueues[i] = create_tasks_queue();
    }
    rr_index = 0;
}

void delete_queues(void)
{
    if (tqueues == NULL)
        return;
    for (int i = 0; i < THREAD_COUNT; i++)
    {
        if (tqueues[i])
            free_tasks_queue(tqueues[i]);
    }
    free(tqueues);
    tqueues = NULL;
}

void *worker_func(void *arg)
{ // consumer
    PRINT_DEBUG(10, "Worker thread started.\n");
    task_t *new_task;
    int worker_id = *(int *)arg;
    while (1)
    {
        pthread_mutex_lock(&mutex);
        int total = 0;
        for (int i = 0; i < THREAD_COUNT; i++)
            total += tqueues[i]->index;
        while (total == 0 && turn_off == 0)
        {
            pthread_cond_wait(&empty, &mutex);
            total = 0;
            for (int i = 0; i < THREAD_COUNT; i++)
                total += tqueues[i]->index;
        }
        
        // check if we have to be turned off

        if (turn_off)
        {
            pthread_mutex_unlock(&mutex);
            break;
        }
        // new task
        new_task = get_task_to_execute(worker_id);
        pthread_mutex_unlock(&mutex);

        if (new_task != NULL)
        {
            active_task = new_task;

            int result = exec_task(new_task);
            if (result == TASK_COMPLETED)
            {
                terminate_task(new_task);
            }
            else if (result == TASK_TO_BE_RESUMED)
            {
                new_task->status = WAITING;
            }
            active_task = NULL;
        }
    }
    return NULL;
}

void create_thread_pool(void)
{
    workers = malloc(sizeof(pthread_t) * THREAD_COUNT);
    if (workers == NULL)
    {
        perror("ALLOCATION ERROR");
    }
    thread_ids = malloc(sizeof(int) * THREAD_COUNT);
    if (thread_ids == NULL)
    {
        perror("ALLOCATION ERROR");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < THREAD_COUNT; i++)
    {
        /* code */
        thread_ids[i] = (int)i;
        if (pthread_create(&workers[i], NULL, worker_func, &thread_ids[i]) != 0)
        {
            perror("THREAD CREATION PROBLEM\n");
        }
    }
}

void dispatch_task(task_t *t) // producer
{
    pthread_mutex_lock(&mutex);
    int target = rr_index++ % THREAD_COUNT;
    if (tqueues[target]->index == tqueues[target]->task_buf_size)
    {
        tqueues[target]->task_buf_size = tqueues[target]->task_buf_size * 2;
        task_t **resized_buffer = realloc(tqueues[target]->task_buffer, sizeof(task_t *) * tqueues[target]->task_buf_size);
        if (resized_buffer == NULL)
        {
            perror("Error while allocating");
            exit(EXIT_FAILURE);
        }
        tqueues[target]->task_buffer = resized_buffer;
    }
    enqueue_task(tqueues[target], t);
    pthread_cond_signal(&empty);
    pthread_mutex_unlock(&mutex);
}

task_t *get_task_to_execute(int worker_id)
{
    task_t *t = dequeue_task(tqueues[worker_id]);
    if (t != NULL)
        return t;

    for (int i = 1; i < THREAD_COUNT; i++)
    {
        int idx = (worker_id + i) % THREAD_COUNT;
        t = dequeue_task(tqueues[idx]);
        if (t != NULL)
            return t;
    }

    return NULL;
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

    if (completed_task_count == submitted_task_count)
    {
        pthread_cond_signal(&all_tasks_done);
    }

#ifdef WITH_DEPENDENCIES

    if (t->parent_task != NULL)
    { // it does  have a parent
        task_t *parent = t->parent_task;
        parent->task_dependency_done++;

        // check if we still have some dependencies
        if (parent->task_dependency_done == parent->task_dependency_count && parent->status == WAITING)
        {
            parent->status = READY;
            parent_to_wake = parent;
        }
    }
#endif
    pthread_mutex_unlock(&mutex);

    if (parent_to_wake != NULL)
    {
        dispatch_task(parent_to_wake);
    }
}

void task_check_runnable(task_t *t)
{
#ifdef WITH_DEPENDENCIES
    pthread_mutex_lock(&mutex);
    int done = t->task_dependency_done == t->task_dependency_count;
    pthread_mutex_unlock(&mutex);

    if (done)
    {
        t->status = READY; // all the childern are finished
        dispatch_task(t);
    }
#endif
}

