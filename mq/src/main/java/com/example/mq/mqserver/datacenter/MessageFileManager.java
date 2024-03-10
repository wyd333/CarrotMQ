package com.example.mq.mqserver.datacenter;

/**
 * Created with IntelliJ IDEA.
 * Description: 通过这个类来对硬盘上的消息进行管理
 * User: 12569
 * Date: 2024-03-10
 * Time: 21:46
 */
public class MessageFileManager {
    /**
     * 定义一个内部类，来表示该队列的统计信息。
     * 优先考虑 static 静态内部类，不会依赖外部类，限制更少
     */
    static public class Stat {
        public int totalCount;  //总消息数，定义成 public。对于简单的类就直接使用成员，定义成public。类似于C的结构体
        public int validCount;  //有效消息数
    }

    /**
     * 预定消息文件所在的目录和文件名，此方法用来获取指定队列对应的消息文件所在路径
     * @param queueName
     * @return
     */
    private String getQueueDir(String queueName) {
        return "./data/" + queueName;
    }

    /**
     * 此方法用来获取该队列消息数据文件的路径
     * 注意，此处queue_data.txt表示的是二进制文件，虽然二进制文件用txt结尾不是很合适，应当用 bin 或 dat 结尾更合适
     * @param queueName
     * @return
     */
    private String getQueueDataPath(String queueName) {
        return getQueueDir(queueName) + "/queue_data.txt";
    }

    /**
     * 此方法用来获取该队列消息统计文件的路径
     * @param queueName
     * @return
     */
    private String getQueueStatPath(String queueName){
        return getQueueDir(queueName) + "queue_stat.txt";
    }
}
