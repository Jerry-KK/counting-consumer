package cn.lethekk.countingconsumer.service;

import cn.lethekk.countingconsumer.entity.UserRequestMsg;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static cn.lethekk.countingconsumer.constant.MqConstant.*;

/**
 * @author lethe
 * @date 2023/5/15 22:58
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class MQConsumer {
    private final RabbitTemplate rabbitTemplate;
    private final Aggregator aggregator;

    private final DBWriter dbWriter;

    //todo 线程池管理，消费者端接受消息确认，批量操作
    @PostConstruct
    public void consumer() throws InterruptedException {
        log.info("消费者方法启动！！！ ");
        new Thread(aggregator::writeToMQ).start();
        new Thread(dbWriter::start).start();
        while (true) {
            Object msgJson = rabbitTemplate.receiveAndConvert(userRequestQueue);
            if(msgJson != null) {
                UserRequestMsg msg = new Gson().fromJson((String) msgJson, UserRequestMsg.class);
                log.info("拉模式接收到消息 ： " + msg.toString());
                aggregator.accept(msg);
                //TimeUnit.SECONDS.sleep(5);
            } else {
                long s = (long)(1000L * Math.random());
                //log.info("未收到消息,休眠等待 " + s + "毫秒");
                TimeUnit.MILLISECONDS.sleep(s);
            }
        }
    }


}
