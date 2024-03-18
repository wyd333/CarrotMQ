package com.example.mq.mqserver.datacenter;

import com.example.mq.common.BinaryTool;
import com.example.mq.common.MqException;
import com.example.mq.mqserver.core.MSGQueue;
import com.example.mq.mqserver.core.Message;

import java.io.*;
import java.util.LinkedList;
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
     * 暂时不需要什么额外的初始化操作，以备后续扩展
     */
    public void init(){

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
        return getQueueDir(queueName) + "/queue_stat.txt";
    }

    private Stat readStat(String queueName) {
        // 消息统计文件是文本文件，可以直接使用 Scanner 来读取文件内容
        Stat stat = new Stat();
        try (InputStream inputStream = new FileInputStream(getQueueStatPath(queueName))) {
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
        synchronized (queue) {
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(getQueueDataPath(queue.getName()), "rw")) {
                // 1-先从文件中读取对应的 Message 数据
                byte[] bufferSrc = new byte[(int) (message.getOffsetEnd() - message.getOffsetBeg())];
                randomAccessFile.seek(message.getOffsetBeg());  // 移动光标
                randomAccessFile.read(bufferSrc);
                // 2-把当前读出来的二进制数据转换回 Message 对象
                Message diskMessage = (Message) BinaryTool.fromBytes(bufferSrc);
                // 3-把 isValid 设置为无效
                // 此处不需要设置内存中 message 的 isValid 为 0 ，因为这个对象马上要在内存中销毁了
                diskMessage.setIsValid((byte) 0x0);
                // 4-重新写入文件
                byte[] bufferDest = BinaryTool.toBytes(diskMessage);
                randomAccessFile.seek(message.getOffsetBeg());  //文件光标会随着读和写移动，此处重新调整光标
                randomAccessFile.write(bufferDest);
                // 通过上述操作，对于文件来说只是有一个字节发生了改变而已
            }
            // 更新统计文件，有效消息个数要-1
            Stat stat = readStat(queue.getName());
            if (stat.validCount > 0) {
                stat.validCount--;
            }
            writeStat(queue.getName(), stat);
        }
    }

    /**
     * 使用这个方法，从文件中读取所有的消息内容，加载到内存中（一个链表里）。
     * 在程序启动的时候进行调用。
     * 不用传入queue对象，因为不需要对queue进行加锁操作。该方法只在程序启动的时候调用，不涉及多线程，因此不用加锁
     * @param queueName
     * @return 返回一个Linkedlist，主要目的是为了方便后序进行的头删操作
     */
    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, MqException, ClassNotFoundException {
        LinkedList<Message> messages = new LinkedList<>();
        try(InputStream inputStream = new FileInputStream(getQueueDataPath(queueName))) {
            try(DataInputStream dataInputStream = new DataInputStream(inputStream)) {
                // 记录当前文件光标
                long currentOffset = 0;
                // 一个文件中包含了很多消息，此处要循环读取
                while(true) {
                    // 1-读取当前消息的长度，可能会读到文件末尾（EOF），readInt()读到文件末尾会抛EOFException异常，这一点和很多流对象不一样
                    int messageSize = dataInputStream.readInt();
                    // 2-按照这个长度，读取消息内容
                    byte[] buffer = new byte[messageSize];
                    int actualSize = dataInputStream.read(buffer);
                    if(messageSize != actualSize) {
                        // 如果不匹配，说明文件有问题，格式错乱了
                        throw new MqException("[MessageFileManager] 文件格式错误！queueName = " + queueName);
                    }
                    // 3-把读到的二进制数据反序列化为 Message 对象
                    Message message = (Message) BinaryTool.fromBytes(buffer);
                    // 4-判定一下这个消息对象是否有效
                    if(message.getIsValid() != 0x1) {
                        // 无效数据，更新offset，并跳过
                        currentOffset += (4 + messageSize);
                        continue;
                    }
                    // 5-有效数据，把message对象加入链表中。加入前填写 offsetBeg 和 offsetEnd
                    // 进行计算 offset 的时候需要知道当前文件光标的位置。DataInputStream不方便计算，因此要手动计算
                    message.setOffsetBeg(currentOffset + 4);
                    message.setOffsetEnd(currentOffset + 4 + messageSize);
                    currentOffset += (4 + messageSize);
                    messages.add(message);
                }
            } catch (EOFException e) {
                // 并非处理异常，而是处理正常的业务逻辑，文件读到末尾readInt会抛出异常
                // catch 中也不需要做什么特殊的事情
                System.out.println("[MessageFileManager] 恢复 Message 数据完成！");
            }
        }
        return messages;
    }

    /**
     * 检查当前是否要针对该队列的消息数据文件进行GC
     * 条件判定：消息总数 > 2000 且 有效消息 / 总消息数 < 0.5
     * @param queueName
     * @return
     */
    public boolean checkGC(String queueName) {
        // 判断是否要GC，是根据总消息数和有效消息数，这两个值都在消息统计文件中
        Stat stat = readStat(queueName);
        return stat.totalCount > 2000 && (double) stat.validCount / (double) stat.totalCount < 0.5;
    }

    public String getQueueDataNewPath(String queueName) {
        return getQueueDir(queueName) + "/queue_data_new.txt";
    }

    /**
     * 通过这个方法真正执行消息数据文件的垃圾回收操作
     * 使用复制算法来完成，先创建一个新的文件，名字为 queue_data_new.txt
     * 把之前消息数据文件中的有效数据都读出来，写到新的文件中；删除旧的文件，再把新的文件改名回 queue_data.txt
     * 最后要更新消息统计文件
     * @param queue 用作锁对象
     */
    public void gc(MSGQueue queue) throws MqException, IOException, ClassNotFoundException {
        synchronized (queue) {
            // gc操作比较耗时，此处统计一下执行的消耗的时间
            long gcBeg = System.currentTimeMillis();

            // 1-创建一个新的文件
            File queueDataNewFile = new File(getQueueDataNewPath(queue.getName()));
            if(queueDataNewFile.exists()) {
                // 正常情况下这个文件是不该存在的，如果存在则说明上一次gc到一半程序意外崩溃了
                throw new MqException("[MessageFileManager] gc 时发现该队列的 queue_data_new 已经存在！queueName = " + queue.getName());
            }
            boolean ok = queueDataNewFile.createNewFile();
            if(!ok) {
                throw new MqException("[MessageFileManager] 创建文件失败！queueDataNewFile = " + queueDataNewFile.getAbsolutePath());
            }

            // 2-从旧的文件中读取出所有的有效消息对象
            LinkedList<Message> messages = loadAllMessageFromQueue(queue.getName());

            // 3-把有效消息写入到新的文件中
            try (OutputStream outputStream = new FileOutputStream(queueDataNewFile)) {
                try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                    for(Message message : messages) {
                        byte[] buffer = BinaryTool.toBytes(message);
                        // 先写 4 个字节消息的长度
                        dataOutputStream.writeInt(buffer.length);
                        dataOutputStream.write(buffer);
                    }
                }
            }

            // 4-删除旧的数据文件，并把新的数据文件重新命名
            File queueDataOldFile = new File(getQueueDataPath(queue.getName()));
            ok = queueDataOldFile.delete();
            if(!ok) {
                throw new MqException("[MessageFileManager] 删除旧的数据文件失败！queueDataOldFile = " + queueDataOldFile.getAbsolutePath());
            }

            // 把 queue_data_new.txt 重命名为 queue_data.txt
            ok = queueDataNewFile.renameTo(queueDataOldFile);
            if(!ok) {
                throw new MqException("[MessageFileManager] 文件重命名失败！queueDataNewFile = "
                        + queueDataNewFile.getAbsolutePath() + ", queueDataOldFile = " + queueDataOldFile.getAbsolutePath());
            }

            // 5-更新消息统计文件
            Stat stat = readStat(queue.getName());
            stat.totalCount = messages.size();
            stat.validCount = messages.size();
            writeStat(queue.getName(), stat);

            long gcEnd = System.currentTimeMillis();
            System.out.println("[MessageFileManager] gc 执行完毕！queueName = " + queue.getName() +
                    ", time = " + (gcEnd-gcBeg) + "ms");
        }
    }
}
