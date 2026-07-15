#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <thread>

#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        int num_threads;
        void worker(int thread_id, IRunnable* runnable, int num_total_tasks);
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        bool spinning;
        std::mutex* mx_spinning;

        int num_threads;
        std::thread* thread_pool;

        IRunnable* runnable;
        int num_total_tasks;

        int num_remaining_tasks;
        std::mutex* mx_num_remaining_tasks;

        int num_finished_tasks;
        std::mutex* mx_num_finished_tasks;

        void worker();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        // -------------- Control Parameters ---------------
        bool spinning;
        std::mutex* mx_spinning;

        int num_threads;
        std::thread* thread_pool;

        bool work_present;
        std::condition_variable* cv_m2w;
        std::mutex* mx_cv_m2w;

        /*
        | work done | work present | valid? |
        -------------------------------------
        |   false   |    false     |  true  |
        |   false   |    true      |  false |
        |   true    |    false     |  true  |
        |   true    |    true      |  true  |
        */

        bool work_done;
        std::condition_variable* cv_w2m;
        std::mutex* mx_cv_w2m;

        // -------------- Data Parameters ---------------------
        IRunnable* runnable;
        int num_total_tasks;

        int num_remaining_tasks;
        std::mutex* mx_num_remaining_tasks;

        int num_finished_tasks;
        std::mutex* mx_num_finished_tasks;

        void worker(int id);
};

#endif
