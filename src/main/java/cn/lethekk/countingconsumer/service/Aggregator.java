package cn.lethekk.countingconsumer.service;

import cn.lethekk.countingconsumer.entity.AggregatorResult;
import cn.lethekk.countingconsumer.entity.UserRequestMsg;
import com.google.gson.Gson;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
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
@Service
public class Aggregator {

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
     * 处理消息存入本地缓存map
     * @param msg
     */
    public void accept(UserRequestMsg msg) {
        LocalDateTime minuteTime = msg.getTime().withSecond(0).withNano(0);
        long minuteTimeNum = minuteTime.toEpochSecond(ZoneOffset.of("+8"));
        String key = msg.getVideoId() + ":" + minuteTimeNum;
        AggregatorResult result = AggregatorResult.builder()
                .videoId(msg.getVideoId())
                .eventTypeCode(msg.getEventTypeCode())
                .minuteTime(minuteTime)
                .count(1)
                .build();
        log.info("开始更新本地缓存,尝试换取读锁");
        readLock.lock();
        AggregatorResult v = inMemoryMap.putIfAbsent(key, result);
        if(v != null) {
            while (true) {
                result.setCount(v.getCount() + result.getCount());
                boolean replace = inMemoryMap.replace(key, v, result);
                if(replace) {
                    log.info("本地缓存更新成功：" + result);
                    break;
                }
                result.setCount(result.getCount() - v.getCount());
                v = inMemoryMap.get(key);
            }
        }
        readLock.unlock();
    }

    //@PostConstruct
    public void writeToMQ() {
        log.info("写入内部MQ方法启动！！！ ");
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


/*    @Data
    @Builder
    static class Task {
        private AggregatorResult result;
    }*/

}
