package com.example.mq.mqserver.datacenter;

import com.example.mq.common.BinaryTool;
import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.io.*;
import java.util.Scanner;

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

    private Stat readStat(String queueName) {
        // 消息统计文件是文本文件，可以直接使用 Scanner 来读取文件内容
        Stat stat = new Stat();
        try (InputStream inputStream = new FileInputStream(getQueueDataPath(queueName))) {
            Scanner scanner = new Scanner(inputStream);
            stat.totalCount = scanner.nextInt();
            stat.validCount = scanner.nextInt();
            return stat;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void writeStat(String queueName, Stat stat) {
        // 使用 PrintWrite 来写文件
        // OutputStream 打开文件默认会把源文件清空，新的覆盖旧的
        try (OutputStream outputStream = new FileOutputStream(getQueueStatPath(queueName))) {
            PrintWriter printWriter = new PrintWriter(outputStream);
            printWriter.write(stat.totalCount + "\t" + stat.validCount);
            printWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建消息目录和文件
     * @param queueName
     * @throws IOException
     */
    public void createQueueFiles(String queueName) throws IOException {
        //1、先创建队列对应的消息目录
        File baseDir = new File(getQueueDir(queueName));
        if(!baseDir.exists()) {
            // 不存在就创建目录
            boolean ok = baseDir.mkdirs();
            if(!ok) {
                throw new IOException("创建目录失败！baseDir = " + baseDir.getAbsolutePath());
            }
        }

        //2、创建队列数据文件
        File queueDataFile = new File(getQueueDataPath(queueName));
        if(!queueDataFile.exists()) {
            boolean ok = queueDataFile.createNewFile();
            if(!ok) {
                throw new IOException("创建文件失败！queueDataFile = " + queueDataFile.getAbsolutePath());
            }
        }
        //3、创建消息统计文件
        File queueStatFile = new File(getQueueStatPath(queueName));
        if(!queueStatFile.exists()) {
            boolean ok = queueStatFile.createNewFile();
            if(!ok) {
                throw new IOException("创建文件失败！queueStatFile = " + queueStatFile.getAbsolutePath());
            }
        }
        //4、给消息统计文件设定初始值：0\t0
        Stat stat = new Stat();
        stat.totalCount = 0;
        stat.validCount = 0;
        writeStat(queueName, stat);
    }

    /**
     * 删除消息的目录和文件
     * 队列是可以删除的，当队列被删除后，对应的消息文件等也要随之删除
     * @param queueName
     */
    public void destroyQueueFiles(String queueName) throws IOException {
        // 删除里面的文件再删除目录
        File queueDataFile = new File(getQueueDataPath(queueName));
        boolean ok1 = queueDataFile.delete();
        File queueStatFile = new File(getQueueStatPath(queueName));
        boolean ok2 = queueStatFile.delete();
        File baseDir = new File(getQueueDir(queueName));
        boolean ok3 = baseDir.delete();
        if(!ok1 || !ok2 || !ok3) {
            // 有任意一个删除失败 -> 整体删除失败
            throw new IOException("删除目录和文件失败！baseDir = " + baseDir.getAbsolutePath());
        }
    }

    /**
     * 检查队列的目录和文件是否存在
     * 后续有生产者给block server生产消息，这个消息可能需要记录到文件上。这取决于这个消息是否需要持久化。
     * @param queueName
     * @return
     */
    public boolean checkFilesExists(String queueName) {
        // 判定队列的数据文件和统计文件是否都存在
        File queueDataFile = new File(getQueueDataPath(queueName));
        if(!queueDataFile.exists()) {
            return false;
        }
        File queueStatFile = new File(getQueueStatPath(queueName));
        return queueStatFile.exists();
    }


    /**
     * 把一个新的消息放入到队列对应的文件中
     * @param queue 表示要把消息写入的队列
     * @param message 要写的消息
     */
    public void sendMessage(MSGQueue queue, Message message) throws MqException, IOException {
        // 1-检查当前要写入的队列对应的文件是否存在
        if(!checkFilesExists(queue.getName())) {
            throw new MqException("[MessageFileManager] 队列对应的文件不存在！queueName = " + queue.getName());
        }
        // 2-把Message对象进行序列化，转成二进制字节数组
        byte[] messageBinary = BinaryTool.toBytes(message);

        synchronized (queue) {
            // 3-获取到当前队列数据文件的长度，以此计算该Message对象的offsetBeg和offsetEnd
            // 把新的Message数据写入队列数据文件的末尾，此时Message对象的offsetBeg就是当前文件长度+4，offsetEnd就是当前文件长度+4+message自身长度
            File queueDataFile = new File(getQueueDataPath(queue.getName()));
            // queueDataFile.length() 获取到文件的长度，以字节为单位
            message.setOffsetBeg(queueDataFile.length() + 4);
            message.setOffsetEnd(queueDataFile.length() + 4 + messageBinary.length);
            // 4-写入消息到数据文件，追加写
            try (OutputStream outputStream = new FileOutputStream(queueDataFile, true)) {
                try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                    // 先写当前消息的长度，占据4个字节的
                    dataOutputStream.writeInt(messageBinary.length);    // 位运算的封装，以把4个字节的长度都写入
                    // 写入消息本体
                    dataOutputStream.write(messageBinary);  // 与约定的格式相同
                }
            }
            // 5-更新消息统计文件
            Stat stat = readStat(queue.getName());
            stat.totalCount++;
            stat.validCount++;
            writeStat(queue.getName(), stat);
        }
    }

    /**
     * 删除消息
     * 这里的删除是逻辑删除, 把 isValid 属性置 0
     * 步骤: 把文件中的数据读出来, 还原回 Message 对象; 把 isValid 改为 0; 把数据写回文件
     * @param queue
     * @param message
     */
    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException {
        // FileInputStream是从文件头开始读写的, 但此处要求随机访问
        // 内存/硬盘是支持随机访问的
        // Java用RandomAccessFile类来随机访问
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(getQueueDataPath(queue.getName()), "rw")) {
            // 1-先从文件中读取对应的 Message 数据
            byte[] bufferSrc = new byte[(int) (message.getOffsetEnd() - message.getOffsetBeg())];
            randomAccessFile.seek(message.getOffsetBeg());  // 移动光标
            randomAccessFile.read(bufferSrc);
            // 2-把当前读出来的二进制数据转换回 Message 对象
            Message diskMessage = (Message) BinaryTool.fromBytes(bufferSrc);
            // 3-把 isValid 设置为无效
            diskMessage.setIsValid((byte) 0x0);
            // 4-重新写入文件
            byte[] bufferDest = BinaryTool.toBytes(diskMessage);
            randomAccessFile.seek(message.getOffsetBeg());  //文件光标会随着读和写移动，此处重新调整光标
            randomAccessFile.write(bufferDest);
        }
    }
}
