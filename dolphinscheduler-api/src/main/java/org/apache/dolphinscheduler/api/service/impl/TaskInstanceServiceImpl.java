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

package org.apache.dolphinscheduler.api.service.impl;

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.FORCED_SUCCESS;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.TASK_INSTANCE;

import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.ProcessInstanceService;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.service.TaskInstanceService;
import org.apache.dolphinscheduler.api.service.UsersService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.TaskExecuteType;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskInstanceMapper;
import org.apache.dolphinscheduler.remote.command.TaskKillRequestCommand;
import org.apache.dolphinscheduler.remote.command.TaskSavePointRequestCommand;
import org.apache.dolphinscheduler.remote.processor.StateEventCallbackService;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.util.*;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import oshi.driver.linux.proc.ProcessStat;

/**
 * task instance service impl
 */
@Service
public class TaskInstanceServiceImpl extends BaseServiceImpl implements TaskInstanceService {

    @Autowired
    ProjectMapper projectMapper;

    @Autowired
    ProjectService projectService;

    @Autowired
    ProcessService processService;

    @Autowired
    TaskInstanceMapper taskInstanceMapper;

    @Autowired
    ProcessInstanceService processInstanceService;

    @Autowired
    UsersService usersService;

    @Autowired
    TaskDefinitionMapper taskDefinitionMapper;

    @Autowired
    private StateEventCallbackService stateEventCallbackService;

    /**
     * query task list by project, process instance, task name, task start time, task end time, task status, keyword paging
     *
     * @param loginUser         login user
     * @param projectCode       project code
     * @param processInstanceId process instance id
     * @param searchVal         search value
     * @param taskName          task name
     * @param stateType         state type
     * @param host              host
     * @param startDate         start time
     * @param endDate           end time
     * @param pageNo            page number
     * @param pageSize          page size
     * @return task list page
     */
    @Override
    public Result queryTaskListPaging(User loginUser,
                                      long projectCode,
                                      Integer processInstanceId,
                                      String processInstanceName,
                                      String processDefinitionName,
                                      String taskName,
                                      String executorName,
                                      String startDate,
                                      String endDate,
                                      String searchVal,
                                      TaskExecutionStatus stateType,
                                      String host,
                                      TaskExecuteType taskExecuteType,
                                      Integer pageNo,
                                      Integer pageSize,
                                      String productName,
                                      String cluster) {
        Result result = new Result();
        Project project = null;
        Status status = null;
        List<Long> projectIds = new ArrayList<>();
        //作业分析标识projectCode == 0
        if (projectCode == 0) {
            //查询用户拥有的项目
            projectIds = projectMapper.queryByProductAndCluster(loginUser.getId(), productName, cluster);
            //没相关项目权限
            if (projectIds.isEmpty()) {
                projectIds.add(0L);
            }
        } else {
            // check user access for project
            project = projectMapper.queryByCode(projectCode);
            Map<String, Object> checkResult =
                    projectService.checkProjectAndAuth(loginUser, project, projectCode, TASK_INSTANCE);
            status = (Status) checkResult.get(Constants.STATUS);
            if (status != Status.SUCCESS) {
                putMsg(result, status);
                return result;
            }
        }
        int[] statusArray = null;
        if (stateType != null) {
            statusArray = new int[]{stateType.getCode()};
        }
        Map<String, Object> checkAndParseDateResult = checkAndParseDateParameters(startDate, endDate);
        status = (Status) checkAndParseDateResult.get(Constants.STATUS);
        if (status != Status.SUCCESS) {
            putMsg(result, status);
            return result;
        }
        Date start = (Date) checkAndParseDateResult.get(Constants.START_TIME);
        Date end = (Date) checkAndParseDateResult.get(Constants.END_TIME);
        Page<TaskInstance> page = new Page<>(pageNo, pageSize);
        PageInfo<Map<String, Object>> pageInfo = new PageInfo<>(pageNo, pageSize);
        //默认查询当前用户
        int executorId = 0;
        if (StringUtils.isNotBlank(executorName)) {
             executorId = usersService.getUserIdByName(executorName);
        } else {
             executorId = usersService.getUserIdByName(loginUser.getUserName());
        }
        IPage<TaskInstance> taskInstanceIPage;
        if (projectCode != 0) {
            if (taskExecuteType == TaskExecuteType.STREAM) {
                // stream task without process instance
                taskInstanceIPage = taskInstanceMapper.queryStreamTaskInstanceListPaging(
                        page, project.getCode(), processDefinitionName, searchVal, taskName, executorId, statusArray, host, taskExecuteType, start, end, productName, cluster, Collections.emptyList()
                );
            } else {
                taskInstanceIPage = taskInstanceMapper.queryTaskInstanceListPaging(
                        page, project.getCode(), processInstanceId, processInstanceName, searchVal, taskName, executorId, statusArray, host, taskExecuteType, start, end, productName, cluster, Collections.emptyList()
                );
            }
        } else {
            if (taskExecuteType == TaskExecuteType.STREAM) {
                // stream task without process instance
                taskInstanceIPage = taskInstanceMapper.queryStreamTaskInstanceListPaging(
                        page, 0L, processDefinitionName, searchVal, taskName, executorId, statusArray, host, taskExecuteType, start, end, productName, cluster, projectIds
                );
            } else {
                taskInstanceIPage = taskInstanceMapper.queryTaskInstanceListPaging(
                        page, 0L, processInstanceId, processInstanceName, searchVal, taskName, executorId, statusArray, host, taskExecuteType, start, end, productName, cluster, projectIds
                );
            }
        }

        Set<String> exclusionSet = new HashSet<>();
        exclusionSet.add(Constants.CLASS);
        exclusionSet.add("taskJson");
        List<TaskInstance> taskInstanceList = taskInstanceIPage.getRecords();
        List<Integer> executorIds =
                taskInstanceList.stream().map(TaskInstance::getExecutorId).distinct().collect(Collectors.toList());
        List<User> users = usersService.queryUser(executorIds);
        Map<Integer, User> userMap = users.stream().collect(Collectors.toMap(User::getId, v -> v));
        for (TaskInstance taskInstance : taskInstanceList) {
            taskInstance.setDuration(DateUtils.format2Duration(taskInstance.getStartTime(), taskInstance.getEndTime()));
            User user = userMap.get(taskInstance.getExecutorId());
            if (user != null) {
                taskInstance.setExecutorName(user.getUserName());
            }
        }
        pageInfo.setTotal((int) taskInstanceIPage.getTotal());
        pageInfo.setTotalList(CollectionUtils.getListByExclusion(taskInstanceIPage.getRecords(), exclusionSet));
        result.setData(pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Result queryTaskInstanceByOaInfo(User loginUser, Long startDate, Long endDate, Integer pageNo, Integer pageSize) {

        Date start =null;

        if(startDate!=null)
        {
            start=new Date(startDate);
        }
        Date end = null;

        if(endDate!=null)
        {
            end=new Date(endDate);
        }


        List<TaskInstance> tasks=this.taskInstanceMapper.queryByOaInfo(start,end);
        Set<Integer> set=new HashSet<>();
        for(TaskInstance task:tasks)
        {
            Integer executor=task.getExecutorId();
            set.add(executor);
        }
        List<User> users=usersService.queryUser(new ArrayList<>(set));

        for(TaskInstance task:tasks)
        {
            for(User user:users)
            {
                if(user.getId()==task.getExecutorId())
                {
                    task.setExecutorName(user.getUserName());
                    break;
                }
            }
        }

        Result<List<TaskInstance>> result = new Result<>();
        result.setData(tasks);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * change one task instance's state from failure to forced success
     *
     * @param loginUser      login user
     * @param projectCode    project code
     * @param taskInstanceId task instance id
     * @return the result code and msg
     */
    @Transactional
    @Override
    public Map<String, Object> forceTaskSuccess(User loginUser, long projectCode, Integer taskInstanceId) {
        Map<String, Object> result = new HashMap<>();
        if (projectCode != 0) {
            Project project = projectMapper.queryByCode(projectCode);
            // check user access for project
            result =
                    projectService.checkProjectAndAuth(loginUser, project, projectCode, FORCED_SUCCESS);
            if (result.get(Constants.STATUS) != Status.SUCCESS) {
                return result;
            }
        }

        // check whether the task instance can be found
        TaskInstance task = taskInstanceMapper.selectById(taskInstanceId);
        if (task == null) {
            putMsg(result, Status.TASK_INSTANCE_NOT_FOUND);
            return result;
        }

        TaskDefinition taskDefinition = taskDefinitionMapper.queryByCode(task.getTaskCode());
        if (projectCode != 0) {
            if (taskDefinition != null && projectCode != taskDefinition.getProjectCode()) {
                putMsg(result, Status.TASK_INSTANCE_NOT_FOUND, taskInstanceId);
                return result;
            }
        } else {
            if (taskDefinition == null) {
                putMsg(result, Status.TASK_INSTANCE_NOT_FOUND, taskInstanceId);
                return result;
            }
        }
        // check whether the task instance state type is failure or cancel
        if (!task.getState().isFailure() && !task.getState().isKill()) {
            putMsg(result, Status.TASK_INSTANCE_STATE_OPERATION_ERROR, taskInstanceId, task.getState().toString());
            return result;
        }

        // change the state of the task instance
        task.setState(TaskExecutionStatus.FORCED_SUCCESS);
        int changedNum = taskInstanceMapper.updateById(task);
        if (changedNum > 0) {
            processService.forceProcessInstanceSuccessByTaskInstanceId(taskInstanceId);
            putMsg(result, Status.SUCCESS);
        } else {
            putMsg(result, Status.FORCE_TASK_SUCCESS_ERROR);
        }
        return result;
    }

    @Override
    public Result taskSavePoint(User loginUser, long projectCode, Integer taskInstanceId) {
        Result result = new Result();
        if (projectCode != 0) {
            Project project = projectMapper.queryByCode(projectCode);
            //check user access for project
            Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectCode, FORCED_SUCCESS);
            Status status = (Status) checkResult.get(Constants.STATUS);
            if (status != Status.SUCCESS) {
                putMsg(result, status);
                return result;
            }
        }
        TaskInstance taskInstance = taskInstanceMapper.selectById(taskInstanceId);
        if (taskInstance == null) {
            putMsg(result, Status.TASK_INSTANCE_NOT_FOUND);
            return result;
        }

        TaskSavePointRequestCommand command = new TaskSavePointRequestCommand(taskInstanceId);

        Host host = new Host(taskInstance.getHost());
        stateEventCallbackService.sendResult(host, command.convert2Command());
        putMsg(result, Status.SUCCESS);

        return result;
    }

    @Override
    public Result stopTask(User loginUser, long projectCode, Integer taskInstanceId) {
        Result result = new Result();
        if (projectCode != 0) {
            Project project = projectMapper.queryByCode(projectCode);
            //check user access for project
            Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectCode, FORCED_SUCCESS);
            Status status = (Status) checkResult.get(Constants.STATUS);
            if (status != Status.SUCCESS) {
                putMsg(result, status);
                return result;
            }
        }
        TaskInstance taskInstance = taskInstanceMapper.selectById(taskInstanceId);
        if (taskInstance == null) {
            putMsg(result, Status.TASK_INSTANCE_NOT_FOUND);
            return result;
        }

        TaskKillRequestCommand command = new TaskKillRequestCommand(taskInstanceId);
        Host host = new Host(taskInstance.getHost());
        stateEventCallbackService.sendResult(host, command.convert2Command());
        putMsg(result, Status.SUCCESS);

        return result;
    }


}
