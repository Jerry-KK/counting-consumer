package cn.lethekk.countingconsumer.entity;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.time.LocalDateTime;

/**
 * @author lethe
 * @date 2023/5/15 22:19
 */
@Data
@Builder
@ToString
public class UserRequestMsg {

    private Long videoId;

    private Integer eventTypeCode;

    private LocalDateTime time;

}
