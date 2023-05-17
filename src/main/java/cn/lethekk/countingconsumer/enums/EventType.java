package cn.lethekk.countingconsumer.enums;

import lombok.Getter;

/**
 * @author lethe
 * @date 2023/5/13 19:50
 */
@Getter
public enum EventType {

    VIEW(1,"code"),   //观看
    LIKE(2,"like"),   //喜欢
    SHARE(3,"share");  //分享


    private int code;
    private String name;

    EventType(int code, String name) {
        this.code = code;
        this.name = name;
    }


}
