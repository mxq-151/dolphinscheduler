package org.apache.dolphinscheduler.common.enums;

public enum CommandState {


    NOT_RUNNING(0),RUNNING(1),ERROR(2),OFFLINE(3);

    int code;
    CommandState(int code){
        this.code=code;
    }

    public int getCode() {
        return code;
    }
}
