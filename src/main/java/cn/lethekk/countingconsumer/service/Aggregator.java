package cn.lethekk.countingconsumer.service;

import cn.lethekk.countingconsumer.entity.AggregatorResult;
import cn.lethekk.countingconsumer.entity.UserRequestMsg;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author lethe
 * @date 2023/5/15 23:37
 */

/**
 * 聚合运算程序
 */

/**
 * 遗留问题:
 * 1 为了避免异常重启等原因导致内部缓存数据丢失，需要对缓存中数据进行持久化。
 * 思路如下，inMemoryMap有一个版本号，todo
 *
 * 2 消息幂等处理
 *
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class Aggregator implements InitializingBean {

    //常量
    public static String internalExchange = "internalExchange";
    public static String internalQueue = "aggregator-internal-queue";
    public static String routingKey = "#";

    public static ConcurrentHashMap<String, AggregatorResult> inMemoryMap = new ConcurrentHashMap<>();
    private static final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    private static Gson gson = new Gson();

    private final RabbitTemplate rabbitTemplate;

    /**
     * 聚合运算任务线程池
     */
    private ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 4,
            10000, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(10000),
            new ThreadFactory() {
                private final AtomicInteger idx = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "pool-" + idx.getAndIncrement());
                }
            }, new ThreadPoolExecutor.AbortPolicy());

    /**
     * 后台写入任务线程池
     */
    private ThreadPoolExecutor writePool = new ThreadPoolExecutor(1, 1,
            10000, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(10),
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "writePoolThread");
                }
            }, new ThreadPoolExecutor.AbortPolicy());

    /*private  static final ThreadPoolExecutor writePool = (ThreadPoolExecutor) Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "writePoolThread");
        }
    });*/

    /**
     * 处理消息存入本地缓存map
     * @param msg
     */
    public boolean accept(UserRequestMsg msg) {
        try {
            pool.execute(new Task(msg));
        } catch (RejectedExecutionException e) {
            log.info("Aggregator中线程池的队列已满，采取拒绝策略");
            return false;
        }
        return true;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("Aggregator 开始工作！！！ ");
        initWrite();
        log.info("Aggregator 完成初始化！！！ ");
    }

    public void initWrite() {
        writePool.execute(new Runnable() {
            @Override
            public void run() {
                writeToMQ();
            }
        });
    }

    private void writeToMQ() {
//        log.info("写入内部MQ方法启动！！！ ");
        while (true) {
            ConcurrentHashMap<String, AggregatorResult> oldMap = inMemoryMap;
            writeLock.lock();
            inMemoryMap = new ConcurrentHashMap<>();
            writeLock.unlock();
            oldMap.values().forEach(aggRes -> {
                String aggResJson = gson.toJson(aggRes);
                rabbitTemplate.convertAndSend(internalExchange, routingKey, aggResJson);
                log.info("本地缓存记录写入内部队列：" + aggResJson);
            });
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    static class Task implements Runnable{
        private UserRequestMsg msg;

        public Task(UserRequestMsg msg) {
            this.msg = msg;
        }

        @Override
        public void run() {
            accept();
        }

        private void accept() {
            LocalDateTime minuteTime = msg.getTime().withSecond(0).withNano(0);
            long minuteTimeNum = minuteTime.toEpochSecond(ZoneOffset.of("+8"));
            String key = msg.getVideoId() + ":" + minuteTimeNum;
            AggregatorResult result = AggregatorResult.builder()
                    .videoId(msg.getVideoId())
                    .eventTypeCode(msg.getEventTypeCode())
                    .minuteTime(minuteTime)
                    .count(1)
                    .build();
            log.info(Thread.currentThread().getName() + " 开始更新本地缓存,尝试换取读锁");
            readLock.lock();
            AggregatorResult v = inMemoryMap.putIfAbsent(key, result);
            if(v != null) {
                while (true) {
                    result.setCount(v.getCount() + result.getCount());
                    boolean replace = inMemoryMap.replace(key, v, result);
                    if(replace) {
                        log.info(Thread.currentThread().getName() + " 本地缓存更新成功：" + result);
                        break;
                    }
                    result.setCount(result.getCount() - v.getCount());
                    v = inMemoryMap.get(key);
                }
            }
            readLock.unlock();
        }
    }

}
