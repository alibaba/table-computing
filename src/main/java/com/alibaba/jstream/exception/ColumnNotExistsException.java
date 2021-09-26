package com.alibaba.jstream.exception;

import javax.annotation.Nonnull;

public class ColumnNotExistsException extends RuntimeException {
    public ColumnNotExistsException(@Nonnull String message) {
        super(message);
    }
}
