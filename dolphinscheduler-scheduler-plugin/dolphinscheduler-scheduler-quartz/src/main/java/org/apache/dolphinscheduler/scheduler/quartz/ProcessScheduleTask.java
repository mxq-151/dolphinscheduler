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

package org.apache.dolphinscheduler.scheduler.quartz;

import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.Schedule;
import org.apache.dolphinscheduler.scheduler.quartz.utils.QuartzTaskUtils;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.util.Date;
import java.util.List;

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;

public class ProcessScheduleTask extends QuartzJobBean {

    private static final Logger logger = LoggerFactory.getLogger(ProcessScheduleTask.class);

    @Autowired
    private ProcessService processService;

    @Counted(value = "ds.master.quartz.job.executed")
    @Timed(value = "ds.master.quartz.job.execution.time", percentiles = {0.5, 0.75, 0.95, 0.99}, histogram = true)
    @Override
    protected void executeInternal(JobExecutionContext context) {
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();

        context.getScheduledFireTime();
        int projectId = dataMap.getInt(QuartzTaskUtils.PROJECT_ID);
        int scheduleId = dataMap.getInt(QuartzTaskUtils.SCHEDULE_ID);

        Date scheduledFireTime = context.getScheduledFireTime();
        Trigger trigger=context.getTrigger();


        Date fireTime = context.getFireTime();

        logger.info("scheduled fire time :{}, fire time :{}, process id :{}", scheduledFireTime, fireTime, scheduleId);

        // query schedule
        Schedule schedule = processService.querySchedule(scheduleId);
        if (schedule == null || ReleaseState.OFFLINE == schedule.getReleaseState()) {
            logger.warn("process schedule does not exist in db or process schedule offline，delete schedule job in quartz, projectId:{}, scheduleId:{}", projectId, scheduleId);
            deleteJob(context, projectId, scheduleId);
            return;
        }

        ProcessDefinition processDefinition = processService.findProcessDefinitionByCode(schedule.getProcessDefinitionCode());
        // release state : online/offline
        ReleaseState releaseState = processDefinition.getReleaseState();
        if (releaseState == ReleaseState.OFFLINE) {
            logger.warn("process definition does not exist in db or offline，need not to create command, projectId:{}, processId:{}", projectId, processDefinition.getId());
            return;
        }


        List<Command> commandList=
                this.processService.queryCommands(schedule.getProcessDefinitionCode(),scheduledFireTime);
        Command command = new Command();
        command.setCommandType(CommandType.SCHEDULER);
        command.setExecutorId(schedule.getUserId());
        command.setManualRun(false);
        command.setScheduleId(scheduleId);
        command.setFailureStrategy(schedule.getFailureStrategy());
        command.setProcessDefinitionCode(schedule.getProcessDefinitionCode());
        command.setScheduleTime(scheduledFireTime);
        command.setStartTime(fireTime);
        command.setWarningGroupId(schedule.getWarningGroupId());
        String workerGroup = StringUtils.isEmpty(schedule.getWorkerGroup()) ? Constants.DEFAULT_WORKER_GROUP : schedule.getWorkerGroup();
        command.setWorkerGroup(workerGroup);
        command.setEnvironmentCode(schedule.getEnvironmentCode());
        command.setWarningType(schedule.getWarningType());
        command.setProcessInstancePriority(schedule.getProcessInstancePriority());
        command.setProcessDefinitionVersion(processDefinition.getVersion());

        boolean find=false;
        for (int j = 0; j < commandList.size(); j++) {
            Command tmp=commandList.get(j);
            if(tmp.getScheduleTime().compareTo(scheduledFireTime)==0)
            {
                find=true;
                break;
            }
        }
        if(!find)
        {
            processService.createCommand(command);
        }


        Date now=new Date();

        //预先创建50个实例
        for (int i = 0; i < 50; i++) {
            scheduledFireTime=trigger.getFireTimeAfter(scheduledFireTime);
            command = new Command();
            command.setScheduleId(scheduleId);
            command.setCommandType(CommandType.SCHEDULER);
            command.setExecutorId(schedule.getUserId());
            command.setFailureStrategy(schedule.getFailureStrategy());
            command.setProcessDefinitionCode(schedule.getProcessDefinitionCode());
            command.setScheduleTime(scheduledFireTime);
            command.setStartTime(fireTime);
            command.setWarningGroupId(schedule.getWarningGroupId());
            workerGroup = StringUtils.isEmpty(schedule.getWorkerGroup()) ? Constants.DEFAULT_WORKER_GROUP : schedule.getWorkerGroup();
            command.setWorkerGroup(workerGroup);
            command.setEnvironmentCode(schedule.getEnvironmentCode());
            command.setWarningType(schedule.getWarningType());
            command.setProcessInstancePriority(schedule.getProcessInstancePriority());
            command.setProcessDefinitionVersion(processDefinition.getVersion());

            find=false;
            for (int j = 0; j < commandList.size(); j++) {
                Command tmp=commandList.get(j);
                if(tmp.getScheduleTime().compareTo(scheduledFireTime)==0)
                {
                    find=true;
                    break;
                }
            }
            if(find)
            {
                continue;
            }

            processService.createCommand(command);
            //不会预先创建超过12小时的实例
            if(scheduledFireTime.getTime()-now.getTime()>12*60*60*1000)
            {
                break;
            }
        }


    }

    private void deleteJob(JobExecutionContext context, int projectId, int scheduleId) {
        final Scheduler scheduler = context.getScheduler();
        JobKey jobKey = QuartzTaskUtils.getJobKey(scheduleId, projectId);
        try {
            if (scheduler.checkExists(jobKey)) {
                logger.info("Try to delete job: {}, projectId: {}, schedulerId", projectId, scheduleId);
                scheduler.deleteJob(jobKey);
            }
        } catch (Exception e) {
            logger.error("Failed to delete job: {}", jobKey);
        }
    }
}
