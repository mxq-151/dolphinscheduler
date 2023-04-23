package org.apache.dolphinscheduler.service.exceptions;

public class ResourceNotExistsException extends RuntimeException {

    public ResourceNotExistsException(String msg) {
        super(msg);
    }
}
