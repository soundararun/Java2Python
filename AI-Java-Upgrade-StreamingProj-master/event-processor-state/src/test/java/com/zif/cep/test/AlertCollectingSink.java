package com.zif.cep.test;


import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.zif.cep.core.Alert;

/**
 * @author Vijay
 *
 */
public class AlertCollectingSink implements SinkFunction<Alert> {

    private static final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private static final Map<Date, Alert> values = new HashMap<>();

    @Override
    public final void invoke(Alert alert, Context context) {
        readWriteLock.writeLock().lock();
        try {
            values.put(alert.getAlertGeneratedTimestamp(), alert);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public Map<Date, Alert> getValues() {
        readWriteLock.readLock().lock();
        try {
            return Collections.unmodifiableMap(values);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public void clear() {
        readWriteLock.writeLock().lock();
        try {
            values.clear();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}
