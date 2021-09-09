package direct;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述：direct类型的交换机，发送消息
 */
public class EmitLogDirect {
    private static final String EXCHANGE_NAME = "direct_logs";

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
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        //发送消息
        String message = "info：Hello World!";

        channel.basicPublish(EXCHANGE_NAME, "info", null, message.getBytes("UTF-8"));
        System.out.println("发送了消息,等级为info,消息内容为：" + message);

        message = "warning：Hello World!";
        channel.basicPublish(EXCHANGE_NAME, "warning", null, message.getBytes("UTF-8"));
        System.out.println("发送了消息,等级为warning,消息内容为：" + message);

        message = "error：Hello World!";
        channel.basicPublish(EXCHANGE_NAME, "error", null, message.getBytes("UTF-8"));
        System.out.println("发送了消息,等级为error,消息内容为：" + message);
        //关闭连接
        channel.close();
        connection.close();
    }
}
