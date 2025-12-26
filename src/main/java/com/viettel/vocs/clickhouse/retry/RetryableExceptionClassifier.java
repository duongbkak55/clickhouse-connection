package com.viettel.vocs.clickhouse.retry;

import java.sql.SQLException;

public final class RetryableExceptionClassifier {

    private RetryableExceptionClassifier() {}

    public static boolean isRetryable(SQLException ex) {

        // SQLState starting with 08 = connection exception
        if (ex.getSQLState() != null && ex.getSQLState().startsWith("08")) {
            return true;
        }

        String msg = ex.getMessage().toLowerCase();

        return msg.contains("timeout")
                || msg.contains("connection")
                || msg.contains("broken")
                || msg.contains("reset")
                || msg.contains("too many")
                || msg.contains("overloaded");
    }
}
