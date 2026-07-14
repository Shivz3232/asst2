#include <chrono>

#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {

    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::worker(int thread_id, IRunnable* runnable, int num_total_tasks) {
    for (int i = thread_id; i < num_total_tasks; i += num_threads) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    std::thread *thread_pool = new std::thread[num_threads];

    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(
            &TaskSystemParallelSpawn::worker,
            this,
            i,
            runnable,
            num_total_tasks
        );
    }

    for (int i = 0; i < num_threads; i++) {
        thread_pool[i].join();
    }

    delete[] thread_pool;
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    spinning = true;
    mx_spinning = new std::mutex();

    num_total_tasks = 0;
    num_remaining_tasks = 0;
    num_finished_tasks = 0;

    mx_num_remaining_tasks = new std::mutex();
    mx_num_finished_tasks = new std::mutex();

    thread_pool = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::worker, this, i);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    mx_spinning->lock();
    spinning = false;
    mx_spinning->unlock();

    for (int i = 0; i < num_threads; i++) {
        thread_pool[i].join();
    }

    delete mx_spinning;
    delete[] thread_pool;

    delete mx_num_remaining_tasks;
    delete mx_num_finished_tasks;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;

    mx_num_remaining_tasks->lock();
    mx_num_finished_tasks->lock();

    num_remaining_tasks = num_total_tasks;
    num_finished_tasks = 0;

    mx_num_finished_tasks->unlock();
    mx_num_remaining_tasks->unlock();

    while (true) {
        mx_num_finished_tasks->lock();
        if (num_finished_tasks == num_total_tasks) {
            mx_num_finished_tasks->unlock();
            break;
        }
        mx_num_finished_tasks->unlock();
    }
}

void TaskSystemParallelThreadPoolSpinning::worker(int id) {
    int task_id;

    while (true) {
        mx_spinning->lock();
        if (!spinning) {
            mx_spinning->unlock();
            break;
        }
        mx_spinning->unlock();

        {
            mx_num_remaining_tasks->lock();
            if (num_remaining_tasks == 0) {
                mx_num_remaining_tasks->unlock();
                continue;
            }

            task_id = num_total_tasks - num_remaining_tasks;

            num_remaining_tasks -= 1;

            mx_num_remaining_tasks->unlock();
        }

        runnable->runTask(task_id, num_total_tasks);

        mx_num_finished_tasks->lock();
        num_finished_tasks += 1;
        mx_num_finished_tasks->unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    cv = new std::condition_variable();
    cv_m = new std::mutex();
    keep_alive = true;
    threads = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::worker, this, i);
    }

    num_total_tasks = 0;
    num_consumed_tasks = 0;
    mx_num_consumed_tasks = new std::mutex();
    runnable = nullptr;
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    // printf("In destructor\n");

    keep_alive = false;

    cv->notify_all();

    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }

    delete cv;
    delete cv_m;
    delete[] threads;

    delete mx_num_consumed_tasks;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelThreadPoolSleeping::worker(int id) {
    while (keep_alive) {
        {
            std::unique_lock<std::mutex> lk(*cv_m);
            cv->wait(lk);
        }

        while (true) {
            int task_id;

            mx_num_consumed_tasks->lock();
            if (num_consumed_tasks == num_total_tasks) {
                mx_num_consumed_tasks->unlock();
                break;
            }

            task_id = num_consumed_tasks;
            num_consumed_tasks += 1;
            mx_num_consumed_tasks->unlock();

            runnable->runTask(task_id, num_total_tasks);
        }

        cv->notify_one();
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
