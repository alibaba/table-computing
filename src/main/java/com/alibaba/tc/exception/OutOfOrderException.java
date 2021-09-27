package com.alibaba.tc.exception;

import javax.annotation.Nonnull;

public class OutOfOrderException extends RuntimeException {
    public OutOfOrderException(@Nonnull String message) {
        super(message);
    }
}
