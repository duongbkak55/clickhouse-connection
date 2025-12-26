package com.viettel.vocs.clickhouse.retry;

@FunctionalInterface
public interface RetryableSupplier<T> {
    T get();
}
