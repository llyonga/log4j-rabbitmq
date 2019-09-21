package cn.llyong.log.appender;

import cn.llyong.comm.MqDataWrapper;
import cn.llyong.disruptor.MessageConsumer;
import cn.llyong.disruptor.MessageProducer;
import cn.llyong.disruptor.RingBufferWorkerPoolFactory;
import cn.llyong.rabbitmq.RabbitmqConsumer;
import com.alibaba.fastjson.JSON;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * @description:
 * @author: llyong
 * @date: 2019/9/21
 * @time: 15:38
 * @version: 1.0
 */
public class RabbitmqAppender extends AppenderSkeleton {

    private String username = "guest";
    private String password = "guest";
    private String host = "localhost";
    private int port = 5762;
    private String virtualHost = "/";
    private String exchange = "amqp-exchange";
    private String type = "direct";
    private boolean durable = false;
    private String queue = "amqp-queue";
    private String routingKey = "";

    private MqDataWrapper mqData;

    private ConnectionFactory factory = new ConnectionFactory();
    private Connection connection = null;
    private Channel channel = null;

    @Override
    protected void append(LoggingEvent event) {
        Properties properties = System.getProperties();
        Map<String, Object> map = new HashMap<>(16);
        map.put("dcn", properties.getProperty("dcn"));
        map.put("timeStamp", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.ofInstant(Instant.ofEpochMilli(event.getTimeStamp()), ZoneId.systemDefault())));
        map.put("level", event.getLevel().toString());
        map.put("threadName", event.getThreadName());
        map.put("className", event.getLoggerName());
        map.put("method", event.getLocationInformation().getMethodName());
        map.put("lineNumber", event.getLocationInformation().getLineNumber());
        map.put("fullLocationInf", event.getLocationInformation().fullInfo);
        map.put("message", event.getMessage());
        map.put("throwable", event.getThrowableInformation());
        String logMessage = JSON.toJSONString(map);

        //自已的应用服务应该有一个ID生成规则
        String producerId = UUID.randomUUID().toString();
        MessageProducer messageProducer = RingBufferWorkerPoolFactory.getInstance().getMessageProducer(producerId);
        messageProducer.onData(logMessage, channel, exchange, routingKey);
//        System.out.println(logMessage);
//        try {
//            //发送消息
//            channel.basicPublish(exchange, routingKey, null, logMessage.getBytes("UTF-8"));
//        } catch (Exception e) {
//            LogLog.error(e.getMessage());
//        }
//        System.out.println("消息已发送！");
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    public void activateOptions() {
        super.activateOptions();
        this.connect();
        initRabbitmqConsumer();
    }

    void connect() {
        //creating connection
        try {
            this.createConnection();
        } catch (Exception e) {
            errorHandler.error(e.getMessage(), e, ErrorCode.GENERIC_FAILURE);
        }

        //creating channel
        try {
            this.createChannel();
        } catch (IOException ioe) {
            errorHandler.error(ioe.getMessage(), ioe, ErrorCode.GENERIC_FAILURE);
        }

        //create exchange
        try {
            this.createExchange();
        } catch (Exception ioe) {
            errorHandler.error(ioe.getMessage(), ioe, ErrorCode.GENERIC_FAILURE);
        }

        //create queue
        try {
            this.createQueue();
        } catch (Exception ioe) {
            errorHandler.error(ioe.getMessage(), ioe, ErrorCode.GENERIC_FAILURE);
        }
    }

    /**
     * 配置连接属性
     */
    private void setFactoryConfiguration() {
        factory.setHost(this.host);
        factory.setPort(this.port);
        factory.setUsername(this.username);
        factory.setPassword(this.password);
        factory.setVirtualHost(this.virtualHost);
    }

    /**
     * 创建一个connection
     * @return
     * @throws IOException
     */
    private Connection createConnection() throws Exception {
        setFactoryConfiguration();
        if (this.connection == null || !this.connection.isOpen()) {
            this.connection = factory.newConnection();
        }
        return this.connection;
    }

    /**
     * 创建一个Channel
     * @return
     * @throws IOException
     */
    private Channel createChannel() throws IOException {
        boolean b = this.channel == null || !this.channel.isOpen() && (this.connection != null && this.connection.isOpen());
        if (b) {
            this.channel = this.connection.createChannel();
        }
        return this.channel;
    }

    /**
     * 创建一个exchange
     * @throws IOException
     */
    private void createExchange() throws IOException {
        if (this.channel != null && this.channel.isOpen()) {
            synchronized (this.channel) {
                this.channel.exchangeDeclare(this.exchange, this.type, this.durable);
            }
        }
    }

    /**
     * 创建一个queue
     * @throws IOException
     */
    private void createQueue() throws IOException {
        if (this.channel != null && this.channel.isOpen()) {
            synchronized (this.channel) {
                this.channel.queueDeclare(this.queue, this.durable, false, false, null);
                this.channel.queueBind(this.queue, this.exchange, this.routingKey);
            }
        }
    }

    @Override
    public void close() {
//        if (!this.closed) {
//            this.closed = true;
//        }
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException | TimeoutException e) {
                errorHandler.error(e.getMessage(), e, ErrorCode.CLOSE_FAILURE);
            }
        }
        if (connection != null && connection.isOpen()) {
            try {
                this.connection.close();
            } catch (IOException ioe) {
                errorHandler.error(ioe.getMessage(), ioe, ErrorCode.CLOSE_FAILURE);
            }
        }
    }

    void initRabbitmqConsumer() {
        MessageConsumer[] consumers = new MessageConsumer[12];
        MessageConsumer messageConsumer;
        int length = consumers.length;
        for (int i = 0; i < length ; i++) {
            messageConsumer = new RabbitmqConsumer(UUID.randomUUID().toString());
            consumers[i] = messageConsumer;
        }

        RingBufferWorkerPoolFactory.getInstance().initAndStart(
                ProducerType.MULTI,
                1024*1024,
                new YieldingWaitStrategy(),
                consumers);
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }
}


