package topic;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述：特定路由键
 */
public class ReceiveLogsTopic1 {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //设置RabbitMQ地址
        factory.setHost("47.93.8.25");
        factory.setUsername("admin");
        factory.setPassword("password");
        //建立连接
        Connection connection = factory.newConnection();
        //获得信道
        Channel channel = connection.createChannel();
        //创建交换机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        //创建非持久，会自动删除的队列，生成一个随机的临时的queue
        String queueName = channel.queueDeclare().getQueue();
        String routingKey = "*.orange.*";
        channel.queueBind(queueName,EXCHANGE_NAME,routingKey);

        System.out.println("开始接收消息");
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("收到消息：" + message +" routingKey:"+envelope.getRoutingKey());
            }
        };
        channel.basicConsume(queueName,true,consumer);
    }
}
