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
import java.util.concurrent.SynchronousQueue;


/**
 * Created by zhengcong on 2017/9/27.
 */
public class ZookUtil {

    private static final String ZOOKEEPER_ADDRESS = "127.0.0.1:2181";   //zookeeper服务器地址

    private static final String PROPERTIES_PATH = "/props";   //配置文件在zookeeper上的节点名

    private static final String CLUSTER_PATH = "/testCluster";

    private static final String LOCK_PATH = "/testLock";

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookUtil.class);

    private static final String fileName = "demo.properties";   //需要动态配置的配置文件

    private static final SynchronousQueue<Integer> lock_queue = new SynchronousQueue<Integer>();

    public static ZooKeeper getZookeeperClient() throws IOException {
        LOGGER.info("init zookeeper client...");
        ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, 10000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() != Event.EventType.None) {
                    LOGGER.info("{}触发了{}事件!", watchedEvent.getPath(), watchedEvent.getType());
                }
            }
        });
        return zooKeeper;
    }

    public static ZooKeeper getZookeeperClient(Watcher watcher) throws IOException {
        LOGGER.info("init zookeeper client by watcher...");
        ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, 10000, watcher);
        return zooKeeper;
    }

    public static String getFilePath() {
        return System.getProperty("user.dir") + "/src/main/resources/" + fileName;

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
            StringBuffer sb = new StringBuffer();
            for (String key : map.keySet()) {
                sb.append(key + "=" + map.get(key)).append("\n");
            }
            fileWriter = new FileWriter(getFilePath());
            fileWriter.write(sb.toString());
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        } catch (KeeperException e) {
            LOGGER.error(e.getMessage());
        } finally {
            try {
                fileWriter.close();
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
            ZooKeeper zk = getZookeeperClient(new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                        LOGGER.info("注册到zk上的服务器集群有变化");
                        try {
                            queue.put(1);
                        } catch (InterruptedException e) {
                            LOGGER.error(e.getMessage());
                        }
                    }
                }
            });
            while (zk.getChildren("/testCluster", true) != null) {
                queue.take();
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
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
            String nodes[] = list.toArray(new String[list.size()]);
            Arrays.sort(nodes);
            if (newNode.equals(LOCK_PATH + "/" + nodes[0])) {  //与zk中最小的锁节点比较，相同则获取锁成功
                LOGGER.info("获取锁成功");
                Thread.sleep(5000);
                zk.close();    //由于创建的锁节点是临时节点，所以客户端退出即删除相应节点
                lock_queue.put(1);
            } else {
                LOGGER.info("获取锁失败，持续等待");
                lock_queue.take();
                zk.close();   //退出客户端以删除获取锁失败时创建的节点
                simulateLock();  //尝试重新获取锁
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

}
