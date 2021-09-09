package workqueues;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述：消费者，接收前面的批量消息
 */
public class Worker {
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
        final Channel channel = connection.createChannel();
        //声明队列
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println("开始接收消息");
        channel.basicQos(1);//最希望处理的数量
        channel.basicConsume(TASK_QUEUE_NAME,false,new DefaultConsumer(channel){//false代表关闭自动确认消息
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                //处理消息
                String message = new String(body,"UTF-8");
                System.out.println("收到了消息:"+message);
                try{
                    doWork(message);
                }finally {
                    System.out.println("消息处理完成");
                    //处理完毕之后，把消息进行确认 --手动确认
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            }
        });
    }

    private static void doWork(String task){
        char[] chars = task.toCharArray();
        for (char ch : chars) {
            if (ch == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
