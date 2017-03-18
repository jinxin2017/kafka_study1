package com.study.zookeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperLeaderElection {
	public static void main(String[] args) throws IOException {
		ZooKeeper zk = new ZooKeeper("kafka0:2181,kafka1:2182,kafka2:2183",
				6000, new Watcher() {
					@Override
					public void process(WatchedEvent event) {
						System.out.println(event.getState());
					}
				});
		try {
			// 先创建持久节点 kafka_homework
			// zk.create("/kafka_homework", null, Ids.OPEN_ACL_UNSAFE,
			// CreateMode.PERSISTENT);
			// 创建临时顺序节点,并获取返回创建的节点名称
			String nodeName = "/kafka_homework/test";
			String returnId = zk.create(nodeName, null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
			// 截取id号
			String currentId = returnId.substring(16, returnId.length());
			selection(zk, currentId);
			Thread.sleep(100000);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static void selection(ZooKeeper zk, String currentId)
			throws KeeperException, InterruptedException {
		// 获取父目录下的所有子节点
		List<String> idsStr = zk.getChildren("/kafka_homework", false);
		Collections.sort(idsStr);
		// 获取当前在子目录下的ID
		int currentIndex = idsStr.indexOf(currentId);
		System.out.println(currentIndex);
		if (currentIndex == 0) {
			System.out.println(currentId + "竞选Leader成功");
		} else {
			// 监控前一个Id
			zk.exists("/kafka_homework/" + idsStr.get(currentIndex - 1),
					new Watcher() {
						@Override
						public void process(WatchedEvent event) {
							try {
								// 如果前一个ID 删除了，则进行处理
								if (event.getType() == EventType.NodeDeleted) {
									System.out.println("判断是否竞选");
									selection(zk, currentId);
								}
								// 否则继续监控
								else {
									zk.exists(
											"/kafka_homework/"
													+ idsStr.get(currentIndex - 1),
											this);
								}
							} catch (KeeperException e) {
								e.printStackTrace();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					});
		}
	}
}
