package cn.lethekk.countingconsumer.entity;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * @author lethe
 * @date 2023/5/16 12:16
 */
@Data
public class VideoPreMinuteRecord {

    /**
     * 自增ID
     */
    private Long id;

    /**
     * 视频ID
     */
    private Long videoId;

    /**
     * 事件类型code
     */
    private Integer eventTypeCode;

    /**
     * 操作时间(精确到分钟)
     */
    private LocalDateTime minuteTime;

    /**
     * 事件类型code
     */
    private Integer count;

}
