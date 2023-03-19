/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.exception;

public class StreamLoadFailException extends Exception {

  public StreamLoadFailException() {
    super();
  }

  public StreamLoadFailException(String message) {
    super(message);
  }

  public StreamLoadFailException(String message, Throwable cause) {
    super(message, cause);
  }

  public StreamLoadFailException(Throwable cause) {
    super(cause);
  }

  protected StreamLoadFailException(String message,
                                    Throwable cause,
                                    boolean enableSuppression,
                                    boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
