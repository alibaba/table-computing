package com.alibaba.jstream.exception;

import javax.annotation.Nonnull;

public class InconsistentColumnTypeException extends RuntimeException {
    public InconsistentColumnTypeException(@Nonnull String message) {
        super(message);
    }
}
