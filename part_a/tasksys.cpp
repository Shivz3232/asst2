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

    num_total_tasks = 0;
    num_tasks_consumed = 0;
    mx_num_tasks_consumed = new std::mutex();

    spinning = true;
    thread_pool = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::worker, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    spinning = false;

    for (int i = 0; i < num_threads; i++) {
        thread_pool[i].join();
    }

    delete mx_num_tasks_consumed;
    delete[] thread_pool;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    this->num_tasks_consumed = 0;

    while (true) {
        mx_num_tasks_consumed->lock();

        if (num_tasks_consumed >= num_total_tasks) {
            num_total_tasks = 0;
            num_tasks_consumed = 0;

            mx_num_tasks_consumed->unlock();

            break;
        }

        mx_num_tasks_consumed->unlock();
    }
}

void TaskSystemParallelThreadPoolSpinning::worker() {
    int task_id;
    while (spinning) {
        mx_num_tasks_consumed->lock();
        if (num_tasks_consumed == num_total_tasks) {
            mx_num_tasks_consumed->unlock();
            continue;
        }

        task_id = num_tasks_consumed;
        num_tasks_consumed += 1;
        mx_num_tasks_consumed->unlock();

        runnable->runTask(task_id, num_total_tasks);
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

    main_2_thread_cv = new std::condition_variable();
    main_2_thread_m = new std::mutex();
    thread_2_main_cv = new std::condition_variable();
    thread_2_main_m = new std::mutex();
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

    {
        std::lock_guard<std::mutex> lk(*main_2_thread_m);
    }
    main_2_thread_cv->notify_all();

    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }

    // printf("Threads joined successfully\n");

    delete main_2_thread_cv;
    delete main_2_thread_m;
    delete thread_2_main_cv;
    delete thread_2_main_m;
    delete[] threads;

    delete mx_num_consumed_tasks;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // printf("Run called\n");

    this->num_total_tasks = num_total_tasks;
    num_consumed_tasks = 0;
    this->runnable = runnable;

    // Notify workers
    main_2_thread_cv->notify_all();

    {
        std::unique_lock<std::mutex> lk(*mx_num_consumed_tasks);
        thread_2_main_cv->wait(lk, [this] {
            return this->num_consumed_tasks == this->num_total_tasks;
        });
    }

    this->num_total_tasks = 0;
    num_consumed_tasks = 0;
    this->runnable = nullptr;
}

void TaskSystemParallelThreadPoolSleeping::worker(int id) {
    while (keep_alive) {
        std::unique_lock<std::mutex> lk(*main_2_thread_m);
        main_2_thread_cv->wait(lk);
        lk.unlock();

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

        thread_2_main_cv->notify_one();
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
