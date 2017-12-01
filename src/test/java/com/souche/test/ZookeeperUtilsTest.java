package com.souche.test;

import com.souche.zook.ZookUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhengcong on 2017/9/27.
 */
public class ZookeeperUtilsTest {

    @Test
    public void testAutoProperties(){

        final ZookUtil zookUtil = new ZookUtil();
        new Thread(new Runnable() {
            public void run() {
                zookUtil.testAutoSetPropertiesFile();  //开启一个线程用于监听远程配置文件的变化并同步
            }
        }).start();
        new Thread(new Runnable() {   //开启一个线程模拟改变远程配置文件
            public void run() {
                try {
                    System.out.println("开始模拟改变zookeeper中存储的配置文件...");
                    Thread.sleep(10000);
                    ZooKeeper zk = ZookUtil.getZookeeperClient();
                    zk.setData("/props","{\"name\":\"kobe Bryant\"}".getBytes(),-1);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testCluster(){

        final ZookUtil zookUtil = new ZookUtil();
        new Thread(new Runnable() {
            public void run() {
                zookUtil.simulateCluster();          //集群监视进程，用于监视集群下节点的状态
            }
        }).start();


        //第一个客户端加入
        new Thread(new Runnable() {        //模拟集群下的服务上下线的进程
            public void run() {
                try {
                    ZooKeeper zk1 = ZookUtil.getZookeeperClient();
                    Thread.sleep(5000);
                    zk1.create("/testCluster/child1","child1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    Thread.sleep(2000);
                    zk1.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        //第二个客户端加入
        new Thread(new Runnable() {       //模拟集群下的服务上下线的进程
            public void run() {
                try {
                    ZooKeeper zk2 = ZookUtil.getZookeeperClient();
                    Thread.sleep(7000);
                    zk2.create("/testCluster/child2","child2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    Thread.sleep(3000);
                    zk2.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        try {
            System.in.read();       //阻塞住主线程模拟集群监视进程持续在后台
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLock(){
        final ZookUtil zookUtil = new ZookUtil();
        new Thread(new Runnable() {    //开启一个获取锁的线程
            public void run() {
                zookUtil.simulateLock();
            }
        }).start();

        new Thread(new Runnable() {    //开启一个获取锁的线程
            public void run() {
                zookUtil.simulateLock();
            }
        }).start();

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDistributedBlockingQueue(){
        ZookUtil zookUtil = new ZookUtil();
        zookUtil.simulateProducer();   //开启producer线程
        zookUtil.simulateCustomer();   //开启customer线程
    }

    @Test
    public void test() throws IOException, KeeperException, InterruptedException {

        ZooKeeper zooKeeper = ZookUtil.getZookeeperClient();
        List<String> cildren = zooKeeper.getChildren("/ttt",false);
        Assert.assertEquals(cildren.size(),0);

    }

}
