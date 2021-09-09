package workqueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述：任务有所耗时，多个任务
 */
public class NewTask {
    //定义队列名字
    public static final String TASK_QUEUE_NAME = "task_queue";

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
        //声明队列
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        for (int i = 0; i < 10; i++) {
            String message;
            if(i%2==0){
                message = i + "...";
            }else{
                message = String.valueOf(i);
            }

            channel.basicPublish("", TASK_QUEUE_NAME, null, message.getBytes("UTF-8"));
            System.out.println("发送了消息：" + message);
        }
        //关闭连接
        channel.close();
        connection.close();
    }
}