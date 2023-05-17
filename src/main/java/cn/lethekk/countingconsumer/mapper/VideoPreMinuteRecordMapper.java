package cn.lethekk.countingconsumer.mapper;

import cn.lethekk.countingconsumer.entity.VideoPreMinuteRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * @author lethe
 * @date 2023/5/16 12:24
 */
@Mapper
public interface VideoPreMinuteRecordMapper {

    int insert(@Param("e") VideoPreMinuteRecord e);

    int insertOrUpdate(@Param("e") VideoPreMinuteRecord e);

}
