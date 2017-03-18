package com.study.kafka;

import java.util.Random;

import org.apache.log4j.Logger;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 随机存储到partitions
 * 
 * @author jinxiaoxin
 *
 */
public class RandomPartitioner implements Partitioner {
	private static final Logger LOG = Logger.getLogger(RandomPartitioner.class);

	public RandomPartitioner(VerifiableProperties verifiableProperties) {
		LOG.error("查看实例个数");
	}

	public int partition(Object key, int numPartitions) {
		Random random = new Random();
		int randomNumber = Math.abs(random.nextInt() % numPartitions);
		return randomNumber;
	}

}
