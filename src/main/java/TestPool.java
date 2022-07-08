import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j(topic = "TestPoll")
public class TestPool {
    public static void main(String[] args) {
        ThreadPool threadPool = new ThreadPool(2, 1000, TimeUnit.MILLISECONDS,1,
                ((queue, task) -> {
                    //1.死等
                    //queue.put(task);

                    //2.带有超时等待
                    //queue.offer(task,500,TimeUnit.MILLISECONDS);

                    //3.调用者抛出异常
                    //throw new RuntimeException("任务执行失败"+task);

                    //4.调用者放弃任务
                    //log.info("任务{}被放弃",task);

                    //5.调用者自己执行
                    log.info("调用者自己执行任务{}", task);
                    task.run();
                }));
        for (int i = 0; i < 5; i++) {
            int j = i;
            threadPool.execute(() -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info("{}", j);
            });
        }
    }
}

//拒绝策略
@FunctionalInterface
interface RejectPolicy<T> {
    void reject(BlockingQueue<T> queue, T task);
}

@Slf4j(topic = "ThreadPoll")
class ThreadPool {
    //1.任务队列
    private BlockingQueue<Runnable> queue;
    //2.核心线程数
    private int coreSize;
    //3.获取任务时的超时时间
    private long timeout;
    //4.超时时间类型
    private TimeUnit timeUnit;
    //5.工作线程集合
    private final HashSet<Worker> workers = new HashSet<>();
    //6.拒绝策略
    private RejectPolicy<Runnable> rejectPolicy;

    public ThreadPool(int coreSize, long timeout, TimeUnit timeUnit, int queueCapacity) {
        this.queue = new BlockingQueue<>(queueCapacity);
        this.coreSize = coreSize;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    public ThreadPool(int coreSize, long timeout, TimeUnit timeUnit, int queueCapacity, RejectPolicy<Runnable> rejectPolicy) {
        this.queue = new BlockingQueue<>(queueCapacity);
        this.coreSize = coreSize;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.rejectPolicy = rejectPolicy;
    }


    // 执行任务
    public void execute(Runnable task) {
        synchronized (workers) {
            if (workers.size() < coreSize) {
                Worker worker = new Worker(task);
                log.info("新增worker{},任务{}", worker, task);
                workers.add(worker);
                worker.start();
            } else {
                queue.tryPut(rejectPolicy, task);
            }
        }
    }

    class Worker extends Thread {
        private Runnable task;

        public Worker(Runnable task) {
            this.task = task;
        }

        @Override
        public void run() {
            while (task != null || (task = queue.poll(timeout, timeUnit)) != null) {
                try {
                    log.info("正在执行...{}", task);
                    task.run();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    task = null;
                }
            }
            synchronized (workers) {
                log.info("{}已经被移除", this);
                workers.remove(this);
            }
        }
    }
}


@Slf4j(topic = "BlockingQueue")
//任务队列类
class BlockingQueue<T> {
    //1.任务队列
    private Deque<T> queue = new ArrayDeque<>();

    //2.锁
    private ReentrantLock lock = new ReentrantLock();

    //3.队列容量
    private int capacity;

    //4.生产者条件变量
    private Condition fullWaitSet = lock.newCondition();

    //5.消费者条件变量
    private Condition emptyWaitSet = lock.newCondition();

    //构造函数
    public BlockingQueue(int capacity) {
        this.capacity = capacity;
    }

    //阻塞获取
    public T take() {
        lock.lock();
        try {
            while (queue.isEmpty()) {
                try {
                    emptyWaitSet.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            T t = queue.poll();
            fullWaitSet.signal();
            return t;
        } finally {
            lock.unlock();
        }
    }

    //带时限的阻塞获取
    public T poll(long timeout, TimeUnit unit) {
        lock.lock();
        try {
            long nanos = unit.toNanos(timeout);
            while (queue.isEmpty()) {
                try {
                    if (nanos <= 0)
                        return null;
                    nanos = emptyWaitSet.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            T t = queue.poll();
            fullWaitSet.signal();
            return t;
        } finally {
            lock.unlock();
        }
    }

    //阻塞添加
    public void put(T task) {
        lock.lock();
        try {
            while (queue.size() == capacity) {
                try {
                    log.info("任务队列已满，{}等待加入任务队列", task);
                    fullWaitSet.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            log.info("{}已加入任务队列", task);
            queue.offer(task);
            emptyWaitSet.signal();
        } finally {
            lock.unlock();
        }
    }

    //带有时限的阻塞添加
    public boolean offer(T task, long timeout, TimeUnit unit) {
        lock.lock();
        try {
            long nanos = unit.toNanos(timeout);
            while (queue.size() == capacity) {
                try {
                    if (nanos <= 0) {
                        log.info("等待超时,{}加入任务队列失败", task);
                        return false;
                    }
                    log.info("任务队列已满，{}等待加入任务队列", task);
                    nanos = fullWaitSet.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            log.info("{}已加入任务队列", task);
            queue.offer(task);
            emptyWaitSet.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    //带有拒绝策略的添加
    public void tryPut(RejectPolicy<T> rejectPolicy, T task) {
        lock.lock();
        try {
            if (queue.size() == capacity) {
                rejectPolicy.reject(this, task);
            }else {
                log.info("{}已加入任务队列", task);
                queue.offer(task);
                emptyWaitSet.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    //获取任务队列的长度
    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }


}
