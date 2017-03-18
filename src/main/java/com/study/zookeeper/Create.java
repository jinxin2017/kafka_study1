package com.study.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Create {
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		ZooKeeper zk=new ZooKeeper("kafka0:2181", 6000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getState());
			}
		});
		zk.create("/chroot", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.exists("/chroot", new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getPath()+"|"+event.getType().name()); 
				try {
					zk.exists("/chroot", this);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
//		Watcher watcher=event->{System.out.println(event.getPath()+"|"+event.getType().name());};
//		zk.exists("/chroot", watcher);
		Thread.sleep(100000);
		zk.close();
	}
}
