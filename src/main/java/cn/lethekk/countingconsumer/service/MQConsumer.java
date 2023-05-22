package cn.lethekk.countingconsumer.service;

import cn.lethekk.countingconsumer.entity.UserRequestMsg;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.lethekk.countingconsumer.constant.MqConstant.*;

/**
 * @author lethe
 * @date 2023/5/15 22:58
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class MQConsumer implements InitializingBean {
    private final RabbitTemplate rabbitTemplate;
    private final Aggregator aggregator;
    private final MessageConverter messageConverter;


    private ThreadPoolExecutor consumerPool = new ThreadPoolExecutor(4, 4,
            1000, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(10),
            new ThreadFactory() {
                private final AtomicInteger idx = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "consumerPool" + idx.getAndIncrement());
                }
            },new ThreadPoolExecutor.AbortPolicy());

    //todo 线程池管理，消费者端接受消息确认，批量操作
    /*@PostConstruct
    public void consumer() throws InterruptedException {
        log.info("消费者方法启动！！！ ");
        while (true) {
            Object msgJson = rabbitTemplate.receiveAndConvert(userRequestQueue);
            if(msgJson != null) {
                UserRequestMsg msg = new Gson().fromJson((String) msgJson, UserRequestMsg.class);
                log.info("拉模式接收到消息 ： " + msg.toString());
                boolean accept = true;
                //boolean accept = aggregator.accept(msg);
                if(!accept) {
                    //todo 任务被拒绝，采取重试等措施
                }
                //TimeUnit.SECONDS.sleep(5);
            } else {
                long s = (long)(1000L * Math.random());
                //log.info("未收到消息,休眠等待 " + s + "毫秒");
                TimeUnit.MILLISECONDS.sleep(s);
            }
        }
    }*/

    /*@PostConstruct
    public void myInit() {
        log.info("MQConsumer 开始工作！！！");
        for (int i = 0; i < 4; i++) {
            consumerPool.execute(new Task(rabbitTemplate, aggregator));
        }
    }*/


    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("MQConsumer 开始工作！！！");
        for (int i = 0; i < 4; i++) {
            consumerPool.execute(new Task(rabbitTemplate, aggregator, messageConverter));
        }
        log.info("MQConsumer 完成初始化！！！");
    }

    @Slf4j
    static class Task implements Runnable {
        private RabbitTemplate rabbitTemplate;
        private Aggregator aggregator;
        private MessageConverter messageConverter;
        ObjectMapper objectMapper;

        public Task(RabbitTemplate rabbitTemplate, Aggregator aggregator,MessageConverter messageConverter) {
            this.rabbitTemplate = rabbitTemplate;
            this.aggregator = aggregator;
            this.messageConverter = messageConverter;

            objectMapper = new ObjectMapper();
            objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
            objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            objectMapper.registerModule(new JavaTimeModule());
        }

        @Override
        public void run() {
            log.info(Thread.currentThread().getName() + "消费者方法启动！！！ ");
            while (true) {
                String res = rabbitTemplate.execute(new ChannelCallback<String>() {
                    @Override
                    public String doInRabbit(Channel channel) throws Exception {

                        //拉取消息
                        GetResponse getResponse = channel.basicGet(userRequestQueue, false);
                        if (getResponse==null){
                            long s = (long)(10000L * Math.random());
                            //log.info("未收到消息,休眠等待 " + s + "毫秒");
                            TimeUnit.MILLISECONDS.sleep(s);
                            return "暂无消息可消费!";
                        }
                        byte[] body = getResponse.getBody();
                        // 创建 ObjectMapper 对象
                        // 将字节数组反序列化为对象
                        UserRequestMsg msg = objectMapper.readValue(body, UserRequestMsg.class);
                        log.info("拉模式接收到消息 ： " + msg);
                        boolean accept = aggregator.accept(msg);
                        if(accept) {
                            channel.basicAck(getResponse.getEnvelope().getDeliveryTag(), false);
                            return "已经确认的消息 " + new String(body);
                        } else {
                            channel.basicReject(getResponse.getEnvelope().getDeliveryTag(),true);
                            return "已经拒绝的消息 " + new String(body);
                        }
                        //确认消息  不批量确认
//                        channel.basicAck(getResponse.getEnvelope().getDeliveryTag(), false);
                        //拒绝消息 第二个参数表示不确认多个还是一个消息，最后一个参数表示不确认的消息是否重新放回队列
//                channel.basicNack(getResponse.getEnvelope().getDeliveryTag(),false,true);
                        //拒绝消息 ，并重新入列
//                channel.basicReject(getResponse.getEnvelope().getDeliveryTag(),true);
                    }
                });
                log.info(Thread.currentThread().getName() + "轮询一次的结果为 ： " + res);
            }


        }
    }
}
