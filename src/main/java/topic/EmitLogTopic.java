package topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述：topic模式交换机，发送消息
 */
public class EmitLogTopic {
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
        //发送消息
        String message = "Animal World!";

        String[] routingKeys = new String[9];
        routingKeys[0] = "quick.orange.rabbit";
        routingKeys[1] = "lazy.orange.elephant";
        routingKeys[2] = "quick.orange.fox";
        routingKeys[3] = "lazy.brown.fox";
        routingKeys[4] = "lazy.pink.rabbit";
        routingKeys[5] = "quick.brown.fox";
        routingKeys[6] = "orange";
        routingKeys[7] = "quick.orange.male.rabbit";
        routingKeys[8] = "lazy.orange.male.rabbit";

        for (int i = 0; i < routingKeys.length; i++) {
            channel.basicPublish(EXCHANGE_NAME, routingKeys[i], null, message.getBytes("UTF-8"));
            System.out.println("发送了：" + message+" routingKey:"+routingKeys[i]);
        }

        //关闭连接
        channel.close();
        connection.close();
    }
}
