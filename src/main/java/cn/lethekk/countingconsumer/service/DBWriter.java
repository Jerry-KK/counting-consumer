package cn.lethekk.countingconsumer.service;

/**
 * @author lethe
 * @date 2023/5/16 11:58
 */

import cn.lethekk.countingconsumer.entity.AggregatorResult;
import cn.lethekk.countingconsumer.entity.VideoPreMinuteRecord;
import cn.lethekk.countingconsumer.mapper.VideoPreMinuteRecordMapper;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static cn.lethekk.countingconsumer.service.Aggregator.internalQueue;

/**
 * DBWriter:负责将内部队列当中的计算结果最终写入到DB
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class DBWriter {

    private static Gson gson = new Gson();
    //常量
    public static String writeExchange = "write-ex-exchange";
    public static String writeQueue = "write-ex-queue";
    public static String routingKey = "#";


    private final VideoPreMinuteRecordMapper mapper;
    private final RabbitTemplate rabbitTemplate;


    public void start(){
        log.info("DBWriter开始工作！！！ ");
        while (true) {
            try {
                write();
            } catch (Exception e) {
                log.error(e.getMessage());
            }

        }

    }


    public void write() throws InterruptedException {
        //1.从Internal Queue中拉取数据
        AggregatorResult aggRes = pullMsg();
        if(aggRes == null) {
            long s = (long)(1000L * Math.random());
            TimeUnit.MILLISECONDS.sleep(s);
            return;
        }
        //2.数据填充(略)
        enrichData();
        //3.写入数据库
        VideoPreMinuteRecord record = new VideoPreMinuteRecord();
        record.setVideoId(aggRes.getVideoId());
        record.setMinuteTime(aggRes.getMinuteTime());
        record.setEventTypeCode(aggRes.getEventTypeCode());
        record.setCount(aggRes.getCount());
        try {
//            int insert = mapper.insert(record);
            int insert = mapper.insertOrUpdate(record);
            log.info("记录写入成功！！！ " + record);
        } catch (Exception e) {
            //4写入异常时，借助死信队列做异常处理 （死信队列有后台程序做重试操作）
            writeExceptionHandling(record);
        }
    }

    private AggregatorResult pullMsg() throws InterruptedException {
        Object msgJson = rabbitTemplate.receiveAndConvert(internalQueue);
        if(msgJson != null) {
            AggregatorResult aggRes = new Gson().fromJson((String) msgJson, AggregatorResult.class);
            return aggRes;
        }
        return null;
    }

    /**
     * 写入DB前，做数据填充操作
     */
    public void enrichData() {

    }

    /**
     * 写入异常时，把异常记录发送到死信队列 dead-letter queue
     */
    public void writeExceptionHandling(VideoPreMinuteRecord record) {
        String recordJson = gson.toJson(record);
        rabbitTemplate.convertAndSend(writeExchange, routingKey, recordJson);
    }



}
