package com.alibaba.tc.exception;

import javax.annotation.Nonnull;

public class ColumnNameConflictException extends RuntimeException {
    public ColumnNameConflictException(@Nonnull String message) {
        super(message);
    }
}
