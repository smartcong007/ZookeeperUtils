package com.souche.zook;

import com.alibaba.fastjson.JSON;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;


/**
 * Created by zhengcong on 2017/9/27.
 */
public class ZookUtil {

    private static final String ZOOKEEPER_ADDRESS = ConfigUtil.getVal("address");   //zookeeper服务器地址

    private static final String PROPERTIES_PATH = ConfigUtil.getVal("properties_path");   //配置文件在zookeeper上的节点名

    private static final String CLUSTER_PATH = ConfigUtil.getVal("cluster_path");  //集群的父目录节点

    private static final String LOCK_PATH = ConfigUtil.getVal("lock_path");  //锁节点的父目录节点

    private static final String QUEUE_PATH = ConfigUtil.getVal("queue_path");  //队列的父目录节点

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookUtil.class);

    private static final String FILENAME = "demo.properties";   //需要动态配置的配置文件

    private static final int queueSize = 10;

    private static final SynchronousQueue<Integer> lock_wait = new SynchronousQueue<Integer>();

    private static final SynchronousQueue<Integer> queue_wait = new SynchronousQueue<Integer>();

    public static ZooKeeper getZookeeperClient() throws IOException {
        LOGGER.info("init zookeeper client...");
        return new ZooKeeper(ZOOKEEPER_ADDRESS, 10000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() != Event.EventType.None) {
                    LOGGER.info("{}触发了{}事件!", watchedEvent.getPath(), watchedEvent.getType());
                }
            }
        });
    }

    public static ZooKeeper getZookeeperClient(Watcher watcher) throws IOException {
        LOGGER.info("init zookeeper client by watcher...");
        return new ZooKeeper(ZOOKEEPER_ADDRESS, 10000, watcher);
    }

    //用于分布式队列的watcher
    public class QueueWatcher implements Watcher {

        CountDownLatch latch;

        public QueueWatcher() {
            latch = new CountDownLatch(1);
        }

        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                LOGGER.info("队列成员发生变更...");
                latch.countDown();
            }
        }

        public void await() throws InterruptedException {
            latch.await();
        }
    }

    //用于集群管理的watcher
    public class ClusterWatcher implements Watcher {

        ZooKeeper zooKeeper;
        SynchronousQueue<Integer> lock = new SynchronousQueue<Integer>();

        public ClusterWatcher() {
            try {
                zooKeeper = getZookeeperClient();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                try {
                    lock.put(1);
                    synchronized (zooKeeper) {
                        int chidren_actual = zooKeeper.getChildren(CLUSTER_PATH, false).size();
                        int children_before = Integer.valueOf(new String(zooKeeper.getData(
                                CLUSTER_PATH, false, null)));
                        zooKeeper.setData(CLUSTER_PATH, String.valueOf(chidren_actual).getBytes(), -1);
                        if (chidren_actual > children_before) {
                            LOGGER.info("集群中有新服务上线...");
                        } else {
                            LOGGER.info("集群中有服务下线...");
                        }
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void await() throws InterruptedException {
            lock.take();
        }
    }

    //获取应用内配置文件的路径
    public static String getFilePath() {
        return System.getProperty("user.dir") + "/src/main/resources/" + FILENAME;

    }


    public static void initPath(String path) {
        try {
            ZooKeeper zooKeeper = getZookeeperClient();
            Stat stat = zooKeeper.exists(path, false);
            if (stat == null) {
                zooKeeper.create(path, path.substring(1).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            zooKeeper.close();
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
            Thread.currentThread().interrupt();
        } catch (KeeperException e) {
            LOGGER.error(e.getMessage());
        }

    }


    //处理zookeeper上配置文件对应节点的更改
    public void handleUpdate() {
        FileWriter fileWriter = null;
        ZooKeeper zk = null;
        try {
            zk = getZookeeperClient();
            byte[] data = zk.getData(PROPERTIES_PATH, false, null);
            String jsonStr = new String(data);
            Map<String, String> map = JSON.parseObject(jsonStr, Map.class);
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                sb.append(entry.getKey() + "=" + entry.getValue()).append("\n");
            }
            fileWriter = new FileWriter(getFilePath());
            fileWriter.write(sb.toString());
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
            Thread.currentThread().interrupt();
        } catch (KeeperException e) {
            LOGGER.error(e.getMessage());
        } finally {
            try {
                if (fileWriter != null) {
                    fileWriter.close();
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    //借助zookeeper实现简单的动态配置
    public void testAutoSetPropertiesFile() {

        initPath(PROPERTIES_PATH);
        ZooKeeper zooKeeper = null;
        final SynchronousQueue<Integer> queue = new SynchronousQueue<Integer>();
        try {
            zooKeeper = getZookeeperClient(new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getType() != Event.EventType.None) {
                        LOGGER.info("配置文件发生了改变,开始同步到本地...");
                        handleUpdate();
                        try {
                            queue.put(1);
                        } catch (InterruptedException e) {
                            LOGGER.error(e.getMessage());
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });
            File file = new File(getFilePath());
            if (!file.exists()) {
                handleUpdate();
            }
            while (zooKeeper.exists(PROPERTIES_PATH, true) != null) {   //此处会持续监听远程配置文件的改动
                queue.take();
            }

        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
            Thread.currentThread().interrupt();
        } catch (KeeperException e) {
            LOGGER.error(e.getMessage());
        }

    }


    //zookeeper模拟集群管理
    public void simulateCluster() {

        initPath(CLUSTER_PATH);
        LOGGER.info("模拟集群已初始化完毕，持续监听注册的客户端状态...");
        final SynchronousQueue<Integer> queue = new SynchronousQueue<Integer>();
        try {
            ClusterWatcher clusterWatcher = new ClusterWatcher();
            ZooKeeper zk = getZookeeperClient(clusterWatcher);
            int serverCounts = zk.getChildren(CLUSTER_PATH, false).size();
            zk.setData(CLUSTER_PATH, String.valueOf(serverCounts).getBytes(), -1);
            while (zk.getChildren(CLUSTER_PATH, true) != null) {
                clusterWatcher.await();
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
            Thread.currentThread().interrupt();
        } catch (KeeperException e) {
            LOGGER.error(e.getMessage());
        }

    }

    //zookeeper实现分布式锁
    public void simulateLock() {

        initPath(LOCK_PATH);
        try {
            ZooKeeper zk = getZookeeperClient();
            long currentTime = System.currentTimeMillis();
            String newNode = zk.create(LOCK_PATH + "/" + currentTime, String.valueOf(currentTime).getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);  //创建锁节点

            List<String> list = zk.getChildren(LOCK_PATH, false);
            String[] nodes = list.toArray(new String[list.size()]);
            Arrays.sort(nodes);
            if (newNode.equals(LOCK_PATH + "/" + nodes[0])) {  //与zk中最小的锁节点比较，相同则获取锁成功
                LOGGER.info("获取锁成功");
                Thread.sleep(5000);
                zk.close();    //由于创建的锁节点是临时节点，所以客户端退出即删除相应节点
                lock_wait.put(1);
            } else {
                LOGGER.info("获取锁失败，持续等待");
                lock_wait.take();
                zk.close();   //退出客户端以删除获取锁失败时创建的节点
                simulateLock();  //尝试重新获取锁
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        } catch (KeeperException e) {
            LOGGER.error(e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }


    //zookeeper实现阻塞队列的生产者
    public void simulateProducer() {

        initPath(QUEUE_PATH);
        try {
            ZooKeeper zk = getZookeeperClient();
            while (zk.exists(QUEUE_PATH, false) != null) {
                QueueWatcher watcher = new QueueWatcher();
                if (zk.getChildren(QUEUE_PATH, watcher).size() >= queueSize) {
                    LOGGER.info("由于队列已满，进入阻塞状态...");
                    watcher.await();
                }
                zk.create(QUEUE_PATH + "/elem-", String.valueOf(System.currentTimeMillis()).getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        } catch (KeeperException e) {
            LOGGER.error(e.getMessage());
        }

    }

    //zookeeper实现阻塞队列的消费者
    public void simulateCustomer() {

        initPath(QUEUE_PATH);
        try {
            ZooKeeper zk = getZookeeperClient();
            while (zk.exists(QUEUE_PATH, false) != null) {
                QueueWatcher watcher = new QueueWatcher();
                List<String> nodes = zk.getChildren(QUEUE_PATH, watcher);
                if (nodes.isEmpty()) {
                    LOGGER.info("由于队列已空，消费者线程进入阻塞状态...");
                    watcher.await();
                    continue;
                } else {
                    String[] products = nodes.toArray(new String[nodes.size()]);
                    Arrays.sort(products);
                    String path = QUEUE_PATH + "/" + products[0];
                    LOGGER.info("模拟处理队列{}中的{}元素对应的数据", QUEUE_PATH, path);
                    Thread.sleep(5000);
                    zk.delete(path, -1);
                    LOGGER.info("处理完后从队列{}移除元素{}", QUEUE_PATH, path);
                }

            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        } catch (KeeperException e) {
            LOGGER.error(e.getMessage());
        }

    }
}
