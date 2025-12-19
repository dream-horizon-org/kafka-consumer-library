package com.kafka.consumer.utils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class BlockingQueueExecutor extends ThreadPoolExecutor {

  public BlockingQueueExecutor(int threadCount) {
    super(
        threadCount,
        threadCount,
        0,
        MILLISECONDS,
        new LinkedBlockingQueue<>(threadCount * 5),
        new DefaultThreadFactory("consumer-thread", false));
  }
}
