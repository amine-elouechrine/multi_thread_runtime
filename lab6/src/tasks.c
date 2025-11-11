#include <stdlib.h>

#include "tasks.h"
#include "tasks_implem.h"
#include "debug.h"
#include "utils.h"

system_state_t sys_state;

__thread task_t *active_task;


void runtime_init(void)
{
    /* a random number generator might be useful towards the end of
       the lab */
    rand_generator_init();

    create_queues();
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&empty, NULL);
    pthread_cond_init(&full,NULL);
    create_thread_pool();

    sys_state.task_counter = 0;    
}

void runtime_init_with_deps(void)
{
#ifndef WITH_DEPENDENCIES
    fprintf(stderr, "ERROR: dependencies are not supported by the runtime. This application cannot be executed\n");
    exit(EXIT_FAILURE);
#endif

    runtime_init();
}



void runtime_finalize(void)
{
    task_waitall();

    PRINT_DEBUG(1, "Terminating ... \t Total task count: %lu \n", sys_state.task_counter);

    //stopping the workers 
    pthread_mutex_lock(&mutex);
        turn_off = 1;
    pthread_mutex_unlock(&mutex);

    pthread_cond_broadcast(&empty);

    // 4. C'est ce que vous avez dit : la boucle join
    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_join(workers[i], NULL);
    }

    // 5. Nettoyer les ressources
    PRINT_DEBUG(1, "Terminating ... \t Total task count: %lu \n", sys_state.task_counter);

    free(workers); 
    delete_queues();

    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&empty);
    pthread_cond_destroy(&full);
    pthread_cond_destroy(&all_tasks_done);
    
    delete_queues();
}


task_t* create_task(task_routine_t f)
{
    task_t *t = malloc(sizeof(task_t));

    t->task_id = ++sys_state.task_counter;    
    t->fct = f;
    t->step = 0;

    t->tstate.input_list = NULL;
    t->tstate.output_list = NULL;

#ifdef WITH_DEPENDENCIES
    t->tstate.output_from_dependencies_list = NULL;
    t->task_dependency_count = 0;
    t->parent_task = NULL;
#endif
    
    t->status = INIT;
    
    PRINT_DEBUG(10, "task created with id %u\n", t->task_id);    
    
    return t;
}

void submit_task(task_t *t)
{
    t->status = READY;

#ifdef WITH_DEPENDENCIES    
    if(active_task != NULL){
        t->parent_task = active_task;
        active_task->task_dependency_count++;
        
        PRINT_DEBUG(100, "Dependency %u -> %u\n", active_task->task_id, t->task_id);
    }
#endif
    
    dispatch_task(t);
}


void task_waitall(void)
{
    pthread_mutex_lock(&mutex);

    while(completed_task_count < submitted_task_count){
        pthread_cond_wait(&all_tasks_done, &mutex);
    }

    pthread_mutex_unlock(&mutex);
}
