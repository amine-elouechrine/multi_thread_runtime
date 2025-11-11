#ifndef __TASKS_IMPLEM_H__
#define __TASKS_IMPLEM_H__
#include <stdio.h>
#include <pthread.h>
#include "tasks_queue.h"


#include "tasks_types.h"
extern tasks_queue_t *tqueue;
extern pthread_t *workers ;
extern pthread_mutex_t mutex;
extern pthread_cond_t empty;
extern pthread_cond_t full;
extern int submitted_task_count;
extern int completed_task_count;
extern pthread_cond_t all_tasks_done;
extern int turn_off;
void create_queues(void);
void delete_queues(void);

void create_thread_pool(void);

void dispatch_task(task_t *t);
task_t* get_task_to_execute(void);
unsigned int exec_task(task_t *t);
void terminate_task(task_t *t);

void task_check_runnable(task_t *t);

#endif
