/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.dao.mapper;

import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.CommandCount;

import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;

/**
 * command mapper interface
 */
public interface CommandMapper extends BaseMapper<Command> {

    /**
     * count command state
     * @param startTime startTime
     * @param endTime endTime
     * @param projectCodeArray projectCodeArray
     * @return CommandCount list
     */
    List<CommandCount> countCommandState(
            @Param("startTime") Date startTime,
            @Param("endTime") Date endTime,
            @Param("projectCodeArray") Long[] projectCodeArray);

    int upsertCommand(@Param("command") Command command);

    void deleteCommandByScheduleId(@Param("scheduleId")int scheduleId);

    int updateCommandState(@Param("commandId")int commandId,@Param("cstate")int cstate);

    List<Command> queryCommandByCommandType();

    int makeCommandOffline(@Param("scheduleId")int scheduleId);

    int makeCommandOnline(@Param("scheduleId")int scheduleId,@Param("processDefinitionVersion") int processDefinitionVersion);

    /**
     * query command page
     * @return
     */
    List<Command> queryCommandPage(@Param("limit") int limit, @Param("offset") int offset);


    List<Command> queryCommands(@Param("processDefinitionCode")long processDefinitionCode,@Param("scheduleTime") Date scheduleTime);


    /**
     * query command page by slot
     * @return command list
     */
    List<Command> queryCommandPageBySlot(@Param("limit") int limit, @Param("offset") int offset,@Param("processDefinitionCode")long processDefinitionCode);

    List<Command> queryProcess( @Param("masterCount") int masterCount, @Param("thisMasterSlot") int thisMasterSlot);

}
