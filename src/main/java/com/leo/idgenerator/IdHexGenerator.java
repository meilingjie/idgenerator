/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.leo.idgenerator;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * id generator
 *
 * @author leo
 * @date 2017/2/22.
 */
public class IdHexGenerator {
    private static final long startTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) / 1000;

    private static final long workerIdBits = 2L;
    private static final long dataCenterIdBits = 2L;
    private static final int maxWorkerId = 99;
    private static final int maxDataCenterId = 99;

    private static final long sequenceBits = 4;
    private static final long workerIdShift = (long) Math.pow(10, sequenceBits);
    private static final long dataCenterIdShift = (long) Math.pow(10, sequenceBits + workerIdBits);
    private static final long timeLeftShift = (long) Math.pow(10, sequenceBits + workerIdBits
            + dataCenterIdBits);
    private static final Random r = new Random();

    private final long workerId;
    private final long dataCenterId;
    private final long idEpoch;
    private long lastTime = -1L;
    private long sequence = 0;

    public IdHexGenerator() {
        this(startTime);
    }

    public IdHexGenerator(long idEpoch) {
        this(r.nextInt(maxWorkerId), r.nextInt(maxDataCenterId), 0, idEpoch);
    }

    public IdHexGenerator(int workerId, int dataCenterId, long sequence) {
        this(workerId, dataCenterId, sequence, startTime);
    }

    public IdHexGenerator(int workerId, int dataCenterId, long sequence, long idEpoch) {
        this.workerId = workerId;
        this.dataCenterId = dataCenterId;
        this.sequence = sequence;
        this.idEpoch = idEpoch;

        if (workerId < 0 || workerId > maxWorkerId) {
            throw new IllegalArgumentException("workerId is illegal: " + workerId);
        }
        if (dataCenterId < 0 || dataCenterId > maxDataCenterId) {
            throw new IllegalArgumentException("dataCenterId is illegal: " + dataCenterId);
        }

        if (idEpoch >= timeGenerator()) {
            throw new IllegalArgumentException("idEpoch is illegal: " + idEpoch);
        }
    }

    public long getDataCenterId() {
        return dataCenterId;
    }

    public long getWorkerId() {
        return workerId;
    }

    public long getTime() {
        return timeGenerator();
    }

    public synchronized long nextId() {
        long time = timeGenerator();
        if (time < lastTime) {
            throw new IllegalArgumentException("Clock moved backwards.");
        }

        if (lastTime == time) {
            sequence = (sequence + 1) % workerIdShift;
            if (sequence == 0) {
                time = tilNextSecond(lastTime);
            }
        } else {
            sequence = 0;
        }

        lastTime = time;
        long id = ((time - idEpoch) * timeLeftShift) + dataCenterId * dataCenterIdShift
                + workerId * workerIdShift + sequence;
        return id;
    }

    public long getIdTime(long id) {
        return idEpoch + (id / timeLeftShift);
    }

    private long tilNextSecond(long lastTime) {
        long time = timeGenerator();
        while (time <= lastTime) {
            time = timeGenerator();
        }

        return time;
    }

    private long timeGenerator() {
        return System.currentTimeMillis() / 1000;
    }

    public static void main(String[] args) throws Exception {
        IdHexGenerator worker = new IdHexGenerator(1, 1, 0);
        ExecutorService executor = Executors.newFixedThreadPool(5);

        CountDownLatch countDownLatch = new CountDownLatch(10000);
        Runnable run = () -> {
            System.out.println(worker.nextId());
            countDownLatch.countDown();
        };

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            executor.execute(run);
        }
        countDownLatch.await();
        System.out.println(System.currentTimeMillis() - startTime);
        executor.shutdown();
    }
}
