package com.example.mq;

import com.example.mq.mqserver.datacenter.DataBaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Created with IntelliJ IDEA.
 * Description: DataBaseManager的单元测试类
 * User: 12569
 * Date: 2024-02-08
 * Time: 22:28
 */

@SpringBootTest
public class DataBaseManagerTest {
    private DataBaseManager dataBaseManager = new DataBaseManager();

    /**
     * 准备工作，每个测试用例执行前调用
     */
    @BeforeEach
    public void setUp(){
        // 在init中通过context对象得到metaMapper实例，所以要先构建出context对象
        MqApplication.context = SpringApplication.run(MqApplication.class);
        // 有了context对象才能进行后续操作
        dataBaseManager.init();
    }

    /**
     * 收尾工作，每个用例执行后调用
     */
    @AfterEach
    public void tearDown(){
        // 把数据库清空，把数据库清空（删除meta.db文件）
        // 不能直接删除，必须先关闭context对象，否则会出错：context对象持有了MetaMapper实例，而MetaMapper实例又打开meta.db文件
        // 即当meta.db被别人打开时删除该文件是不会成功的，是Windows系统的限制，Linux没有问题
        // 另一方面获取context操作会占用8080端口，此处close也是释放8080端口
        MqApplication.context.close();
        dataBaseManager.deleteDB();
    }
}
