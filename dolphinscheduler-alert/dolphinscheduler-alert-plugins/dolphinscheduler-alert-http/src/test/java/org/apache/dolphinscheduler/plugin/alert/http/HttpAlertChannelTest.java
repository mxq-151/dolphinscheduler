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

import org.apache.dolphinscheduler.alert.api.AlertData;
import org.apache.dolphinscheduler.alert.api.AlertInfo;
import org.apache.dolphinscheduler.alert.api.AlertResult;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.params.PluginParamsTransfer;
import org.apache.dolphinscheduler.spi.params.base.PluginParams;
import org.apache.dolphinscheduler.spi.params.base.Validate;
import org.apache.dolphinscheduler.spi.params.input.InputParam;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class HttpAlertChannelTest {

    @Test
    public void processTest() {

        HttpAlertChannel alertChannel = new HttpAlertChannel();
        AlertInfo alertInfo = new AlertInfo();
        AlertData alertData = new AlertData();
        alertData.setContent("Fault tolerance warning");
        alertInfo.setAlertData(alertData);
        AlertResult alertResult = alertChannel.process(alertInfo);
        Assert.assertEquals("http params is null", alertResult.getMessage());
    }

    @Test
    public void processTest2() {

        HttpAlertChannel alertChannel = new HttpAlertChannel();
        AlertInfo alertInfo = new AlertInfo();
        AlertData alertData = new AlertData();
        alertData.setContent("Fault tolerance warning");
        alertInfo.setAlertData(alertData);
        Map<String, String> paramsMap = PluginParamsTransfer.getPluginParamsMap(getParams());
        alertInfo.setAlertParams(paramsMap);
        AlertResult alertResult = alertChannel.process(alertInfo);
        Assert.assertEquals("true", alertResult.getStatus());
    }

    @Test
    public void processFailTest() {
        HttpAlertChannel alertChannel = new HttpAlertChannel();
        AlertInfo alertInfo = new AlertInfo();
        AlertData alertData = new AlertData();
        alertData.setProcessDesc("affect-data:t_dws_tysl_device_quality");
        alertData.setContent("[{\"projectCode\":13551860373440,\"projectName\":\"ehome_reportAnalysis\",\"owner\":\"admin\",\"processId\":1122111,\"processDefinitionCode\":14075724659136,\"processName\":\"ehome_reportAnalysis_B_reqinterfaceAppidDaySecond_lizc_11_prod-12-20250107164755909\",\"processType\":\"SCHEDULER\",\"processState\":\"SUCCESS\",\"recovery\":\"NO\",\"runTimes\":1,\"processStartTime\":\"2025-01-06 17:10:02\",\"processEndTime\":\"2025-01-06 17:10:18\",\"processHost\":\"sz-bigdata-master-04:5678\"}]");
        alertInfo.setAlertData(alertData);
        Map<String, String> paramsMap = PluginParamsTransfer.getPluginParamsMap(getParams2());
        alertInfo.setAlertParams(paramsMap);
        AlertResult alertResult = alertChannel.process(alertInfo);
        Assert.assertEquals("true", alertResult.getStatus());
    }

    @Test
    public void processTimeoutTest() {
        HttpAlertChannel alertChannel = new HttpAlertChannel();
        AlertInfo alertInfo = new AlertInfo();
        AlertData alertData = new AlertData();
        alertData.setNeedAlert(true);
        alertData.setUser("lizc");
        alertData.setAlertType(AlertType.PROCESS_INSTANCE_TIMEOUT.getCode());
        alertData.setProcessDesc("affect-data:t_dws_tysl_device_quality");
        alertData.setContent(" [{\"projectCode\":9250256554272,\"projectName\":\"ehome_etl\",\"owner\":\"admin\",\"processId\":1119004,\"processDefinitionCode\":14719135812160,\"processName\":\"public_etl_loadAibusinessData_zoujc_6_prod-5-20250106060008998\",\"taskCode\":14719102032064,\"taskName\":\"check\",\"event\":\"TIME_OUT\",\"warnLevel\":\"MIDDLE\",\"taskType\":\"SHELL\",\"taskStartTime\":\"2025-01-06 06:00:15\",\"taskHost\":\"sz-bigdata-coordinate-02:1234\"}]");
        alertInfo.setAlertData(alertData);
        Map<String, String> paramsMap = PluginParamsTransfer.getPluginParamsMap(getParams2());
        alertInfo.setAlertParams(paramsMap);
        AlertResult alertResult = alertChannel.process(alertInfo);
        Assert.assertEquals("true", alertResult.getStatus());
    }

    @Test
    public void serviceFailTest() {
        HttpAlertChannel alertChannel = new HttpAlertChannel();
        AlertInfo alertInfo = new AlertInfo();
        AlertData alertData = new AlertData();
        alertData.setNeedAlert(true);
        alertData.setUser("moxq");
        alertData.setAlertType(AlertType.FAULT_TOLERANCE_WARNING.getCode());
        alertData.setProcessDesc("affect-data:t_dws_tysl_device_quality");
        alertData.setContent(" [{\"type\":\"WORKER\",\"host\":\"/nodes/worker/sz-bigdata-coordinate-01:1234\",\"event\":\"SERVER_DOWN\",\"warningLevel\":\"SERIOUS\"}] ");
        alertInfo.setAlertData(alertData);
        Map<String, String> paramsMap = PluginParamsTransfer.getPluginParamsMap(getParams2());
        alertInfo.setAlertParams(paramsMap);
        AlertResult alertResult = alertChannel.process(alertInfo);
        Assert.assertEquals("true", alertResult.getStatus());
    }


    private String getParams2() {

        List<PluginParams> paramsList = new ArrayList<>();
        InputParam urlParam = InputParam.newBuilder("url", "url")
                .setValue("http://sz3-desk-alert.ctseelink.cn/api/desk/alert/add")
                .addValidate(Validate.newBuilder().setRequired(true).build())
                .build();

        InputParam headerParams = InputParam.newBuilder("headerParams", "headerParams")
                .addValidate(Validate.newBuilder().setRequired(true).build())
                .setValue("{\"Content-Type\":\"application/json\"}")
                .build();

        InputParam bodyParams = InputParam.newBuilder("bodyParams", "bodyParams")
                .addValidate(Validate.newBuilder().setRequired(true).build())
                .setValue("{}")
                .build();

        InputParam content = InputParam.newBuilder("contentField", "contentField")
                .setValue("unit-alert:lizc:李忠财,liurh:刘润浩,dengcy:邓操宇,pengjy:彭金原,zoujch:邹京辰,zoujc:邹京辰")
                .addValidate(Validate.newBuilder().setRequired(true).build())
                .build();

        InputParam requestType = InputParam.newBuilder("requestType", "requestType")
                .setValue("POST")
                .addValidate(Validate.newBuilder().setRequired(true).build())
                .build();

        paramsList.add(urlParam);
        paramsList.add(headerParams);
        paramsList.add(bodyParams);
        paramsList.add(content);
        paramsList.add(requestType);

        return JSONUtils.toJsonString(paramsList);
    }

    /**
     * create params
     */
    private String getParams() {

        List<PluginParams> paramsList = new ArrayList<>();
        InputParam urlParam = InputParam.newBuilder("url", "url")
                                        .setValue("http://www.baidu.com")
                                        .addValidate(Validate.newBuilder().setRequired(true).build())
                                        .build();

        InputParam headerParams = InputParam.newBuilder("headerParams", "headerParams")
                                            .addValidate(Validate.newBuilder().setRequired(true).build())
                                            .setValue("{\"Content-Type\":\"application/json\"}")
                                            .build();

        InputParam bodyParams = InputParam.newBuilder("bodyParams", "bodyParams")
                                          .addValidate(Validate.newBuilder().setRequired(true).build())
                                          .setValue("{\"number\":\"13457654323\"}")
                                          .build();

        InputParam content = InputParam.newBuilder("contentField", "contentField")
                                       .setValue("content")
                                       .addValidate(Validate.newBuilder().setRequired(true).build())
                                       .build();

        InputParam requestType = InputParam.newBuilder("requestType", "requestType")
                                           .setValue("POST")
                                           .addValidate(Validate.newBuilder().setRequired(true).build())
                                           .build();

        paramsList.add(urlParam);
        paramsList.add(headerParams);
        paramsList.add(bodyParams);
        paramsList.add(content);
        paramsList.add(requestType);

        return JSONUtils.toJsonString(paramsList);
    }

}
