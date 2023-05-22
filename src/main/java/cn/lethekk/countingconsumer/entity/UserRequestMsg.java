package cn.lethekk.countingconsumer.entity;

import lombok.*;

import java.time.LocalDateTime;

/**
 * @author lethe
 * @date 2023/5/15 22:19
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@ToString
public class UserRequestMsg {

    private Long videoId;

    private Integer eventTypeCode;

    private LocalDateTime time;

}
