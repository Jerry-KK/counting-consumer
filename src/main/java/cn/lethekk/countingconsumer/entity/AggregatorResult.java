package cn.lethekk.countingconsumer.entity;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * @author lethe
 * @date 2023/5/15 23:49
 */
@Data
@Builder
public class AggregatorResult {

    private long id;
    /**
     * 视频ID
     */
    private long videoId;

    /**
     * 事件类型code
     */
    private int eventTypeCode;

    /**
     * 操作时间(精确到分钟)
     */
    private LocalDateTime minuteTime;

    /**
     * 事件类型code
     */
    private int count;

}
