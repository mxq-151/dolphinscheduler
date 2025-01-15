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

package org.apache.dolphinscheduler.plugin.alert.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.dolphinscheduler.alert.api.AlertChannel;
import org.apache.dolphinscheduler.alert.api.AlertData;
import org.apache.dolphinscheduler.alert.api.AlertInfo;
import org.apache.dolphinscheduler.alert.api.AlertResult;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public final class HttpAlertChannel implements AlertChannel {

    public static String ALERT_TAG="unit-alert";
    public static String DESC_TAG="affect-data";

    private static final Logger logger = LoggerFactory.getLogger(HttpAlertChannel.class);
    @Override
    public AlertResult process(AlertInfo alertInfo) {
        AlertData alertData = alertInfo.getAlertData();
        Map<String, String> paramsMap = alertInfo.getAlertParams();
        if (null == paramsMap) {
            return new AlertResult("false", "http params is null");
        }

        String contentField = paramsMap.get(HttpAlertConstants.NAME_CONTENT_FIELD);
        String[] relTable=null;

        logger.info(JSONUtils.toJsonString(alertData));
        if(contentField.startsWith(ALERT_TAG))
        {
            String names=contentField.substring(ALERT_TAG.length()+1);
            String[] strs= names.split(",");
            String alertLevel="P2";
            String user="莫旭强";
            String desc="";
            int alertType=alertInfo.getAlertData().getAlertType();
            if(alertType== AlertType.FAULT_TOLERANCE_WARNING.getCode())
            {
                JsonNode content=JSONUtils.parseArray(alertData.getContent()).get(0);
                String type = content.get("type").asText();
                String host = content.get("host").asText();
                if("WORKER".equals(type) || "MASTER".equals(type) )
                {
                    alertLevel="P1";
                }else {
                    alertLevel="P2";
                }
                desc=type+"服务节点("+host+")异常";
            }else if((alertType== AlertType.PROCESS_INSTANCE_FAILURE.getCode() ||alertType== AlertType.PROCESS_INSTANCE_TIMEOUT.getCode()|| alertType== AlertType.TASK_FAILURE.getCode()) && alertData.isNeedAlert()) {
                JsonNode content=JSONUtils.parseArray(alertData.getContent()).get(0);
                String projectName=content.get("projectName").asText();
                String processName=content.get("processName").asText();
                if(projectName.equals("dam_etl"))
                {
                    user="江杰锋";
                }else if (projectName.equals("ftp_ingest") || projectName.equals("hadoop_admin"))
                {
                    user="莫旭强";
                }else {

                    boolean match=false;
                    String name= alertData.getUser();
                    for(String str:strs)
                    {
                        String[] tmp=str.split(":");
                        if(name!=null && name.equals(tmp[0]))
                        {
                            user=tmp[1];
                            match=true;
                            break;
                        }
                    }
                    if(!match)
                    {
                        user="邹京辰";
                    }

                    if(processName.toUpperCase().contains("_S_") )
                    {
                        alertLevel="P0";
                    }else if(processName.toUpperCase().contains("_A_") )
                    {
                        alertLevel="P1";
                    }else if(processName.toUpperCase().contains("_B_") || processName.toUpperCase().contains("_C_") || processName.toUpperCase().contains("_D_") )
                    {
                        alertLevel="P2";
                    }
                    else {
                        alertLevel="P2";
                    }
                }

                desc="\n    项目: "+projectName+"\n    工作流: "+processName;

                String processDesc=alertData.getProcessDesc();
                if(processDesc.startsWith(DESC_TAG))
                {
                    String influence="";
                    influence=processDesc.substring(DESC_TAG.length()+1);
                    relTable=influence.split(",");
                }


            }

            ArrayNode alerts=JSONUtils.createArrayNode();
            ObjectNode label=JSONUtils.createObjectNode();
            label.put("alertname","DS任务告警");
            label.put("instance","192.168.241.170");
            label.put("handler",user);
            label.put("severity",alertLevel);
            if(relTable!=null)
            {
                ArrayNode array=JSONUtils.createArrayNode();
                for(String str:relTable)
                {
                    array.add(str);
                }
                label.put("relTable",array);
            }

            label.put("cluster","ds-public");
            label.put("group","ds-public");
            label.put("status","失败");

            // 定义日期时间格式化器
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");
            ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Shanghai"));
            String formattedDateTime = now.format(formatter);

            ObjectNode annotations=JSONUtils.createObjectNode();
            annotations.put("description",desc);
            annotations.put("summary","DS任务告警");
            ObjectNode total=JSONUtils.createObjectNode();
            total.put("annotations",annotations);
            total.put("labels",label);
            total.put("startsAt",formattedDateTime);
            total.put("endsAt",formattedDateTime);
            total.put("receiver","莫旭强");
            total.put("handler",user);
            total.put("status","失败");
            alerts.add(total);

            ObjectNode request=JSONUtils.createObjectNode();
            request.put("alerts",alerts);
            ObjectNode groupseverity=JSONUtils.createObjectNode();
            groupseverity.put("severity",alertLevel);
            request.put("groupLabels",groupseverity);
            request.put("receiver","莫旭强");
            request.put("status","失败");

            logger.info(JSONUtils.toJsonString(request));
            return new HttpSender(paramsMap).send(JSONUtils.toJsonString(request));
        }

        return new HttpSender(paramsMap).send(alertData.getContent());
    }

    public static void printStack()
    {
        StringBuilder sb=new StringBuilder();
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : stackTraceElements) {
            sb.append(element).append("\n");
        }
        logger.info(sb.toString());
    }
}
