package com.viettel.vocs.clickhouse.retry;

public final class RetryExecutor {

    private RetryExecutor() {}

    public static <T> T execute(
            RetryPolicy policy,
            RetryableSupplier<T> supplier
    ) {
        long backoff = policy.getInitialBackoffMs();
        int attempt = 1;

        while (true) {
            try {
                return supplier.get();
            } catch (Exception e) {
                if (attempt >= policy.getMaxAttempts()) {
                    throw e;
                }

                sleep(backoff);

                backoff = Math.min(
                        (long) (backoff * policy.getMultiplier()),
                        policy.getMaxBackoffMs()
                );
                attempt++;
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Retry interrupted", ie);
        }
    }
}
