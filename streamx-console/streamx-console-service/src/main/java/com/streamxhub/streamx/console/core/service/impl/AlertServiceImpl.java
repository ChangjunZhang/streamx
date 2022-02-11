/*
 * Copyright (c) 2019 The StreamX Project
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamxhub.streamx.console.core.service.impl;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.streamxhub.streamx.common.enums.ExecutionMode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streamxhub.streamx.common.util.DateUtils;
import com.streamxhub.streamx.common.util.HadoopUtils;
import com.streamxhub.streamx.common.util.HttpClientUtils;
import com.streamxhub.streamx.common.util.Utils;
import com.streamxhub.streamx.console.core.entity.Application;
import com.streamxhub.streamx.console.core.entity.SenderEmail;
import com.streamxhub.streamx.console.core.enums.CheckPointStatus;
import com.streamxhub.streamx.console.core.enums.FlinkAppState;
import com.streamxhub.streamx.console.core.metrics.flink.MailTemplate;
import com.streamxhub.streamx.console.core.service.AlertService;
import com.streamxhub.streamx.console.core.service.SettingService;
import freemarker.template.Configuration;
import freemarker.template.Template;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.HtmlEmail;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.io.File;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author benjobs
 */
@Slf4j
@Service
public class AlertServiceImpl implements AlertService {

    private Template template;

    @Autowired
    private SettingService settingService;

    private SenderEmail senderEmail;

    /**
     * 存储每个任务的上次告警时间，避免重复报警
     */
    private static final Map<Long, Long> LAST_ALERT_TIME_MAP = new ConcurrentHashMap<>(0);

    //默认告警间隔，5min
    private static final Long ALERT_INTERVAL = 1000L * 60 * 5;
    @PostConstruct
    public void initConfig() throws Exception {
        Configuration configuration = new Configuration(Configuration.VERSION_2_3_28);
        String template = "email.html";
        Enumeration<URL> urls = ClassLoader.getSystemResources(template);
        if (urls != null) {
            if (!urls.hasMoreElements()) {
                urls = Thread.currentThread().getContextClassLoader().getResources(template);
            }

            if (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url.getPath().contains(".jar")) {
                    configuration.setClassLoaderForTemplateLoading(Thread.currentThread().getContextClassLoader(), "");
                } else {
                    File file = new File(url.getPath());
                    configuration.setDirectoryForTemplateLoading(file.getParentFile());
                }
                configuration.setDefaultEncoding("UTF-8");
                this.template = configuration.getTemplate(template);
            }
        } else {
            log.error("email.html not found!");
            throw new ExceptionInInitializerError("email.html not found!");
        }
    }

    @Override
    public void alert(Application application, FlinkAppState appState) {
        long currentTimeMillis = System.currentTimeMillis();
        Long lastAlertTime = LAST_ALERT_TIME_MAP.getOrDefault(application.getId(), currentTimeMillis);
        long alertTimeDiff = currentTimeMillis - lastAlertTime;
        if (alertTimeDiff == 0 || alertTimeDiff >= ALERT_INTERVAL) {
            if (this.senderEmail == null) {
                this.senderEmail = settingService.getSenderEmail();
            }
            if (this.senderEmail != null && (Utils.notEmpty(application.getAlertEmail()) || Utils.notEmpty(application.getFsWebhook()))) {
                MailTemplate mail = getMailTemplate(application);
                mail.setType(1);
                mail.setTitle(String.format("Notify: %s %s", application.getJobName(), appState.name()));
                mail.setStatus(appState.name());

                String subject = String.format("StreamX Alert: %s %s", application.getJobName(), appState.name());
                String[] emails = application.getAlertEmail().split(",");
                String[] fsWebhooks = application.getFsWebhook().split(",");
                sendEmail(mail, subject, emails);
                sendFsMsg(mail, subject, fsWebhooks);
                if (application.getState() == FlinkAppState.CANCELED.getValue() || application.getState() == FlinkAppState.FAILED.getValue()  || application.getState() == FlinkAppState.LOST.getValue()) {
                    LAST_ALERT_TIME_MAP.remove(application.getId());
                } else {
                    LAST_ALERT_TIME_MAP.put(application.getId(), currentTimeMillis);
                }
            }
        }

    }

    @Override
    public void alert(Application application, CheckPointStatus checkPointStatus) {
        long currentTimeMillis = System.currentTimeMillis();
        Long lastAlertTime = LAST_ALERT_TIME_MAP.getOrDefault(application.getId(), currentTimeMillis);
        long alertTimeDiff = currentTimeMillis - lastAlertTime;
        if (alertTimeDiff == 0 || alertTimeDiff >= ALERT_INTERVAL) {
            if (this.senderEmail == null) {
                this.senderEmail = settingService.getSenderEmail();
            }
            if (this.senderEmail != null && (Utils.notEmpty(application.getAlertEmail()) || Utils.notEmpty(application.getFsWebhook()))) {
                MailTemplate mail = getMailTemplate(application);
                mail.setType(2);
                mail.setCpFailureRateInterval(DateUtils.toRichTimeDuration(application.getCpFailureRateInterval()));
                mail.setCpMaxFailureInterval(application.getCpMaxFailureInterval());
                mail.setTitle(String.format("Notify: %s checkpoint FAILED", application.getJobName()));

                String subject = String.format("StreamX Alert: %s, checkPoint is Failed", application.getJobName());
                String[] emails = application.getAlertEmail().split(",");
                String[] fsWebhooks = application.getFsWebhook().split(",");
                sendEmail(mail, subject, emails);
                sendFsMsg(mail, subject, fsWebhooks);
                if (application.getState() == FlinkAppState.CANCELED.getValue() || application.getState() == FlinkAppState.FAILED.getValue()  || application.getState() == FlinkAppState.LOST.getValue()) {
                    LAST_ALERT_TIME_MAP.remove(application.getId());
                } else {
                    LAST_ALERT_TIME_MAP.put(application.getId(), currentTimeMillis);
                }

            }
        }
    }

    private void sendEmail(MailTemplate mail, String subject, String... mails) {
        log.info(subject);
        try {
            Map<String, MailTemplate> out = new HashMap<>(16);
            out.put("mail", mail);

            StringWriter writer = new StringWriter();
            template.process(out, writer);
            String html = writer.toString();
            writer.close();

            HtmlEmail htmlEmail = new HtmlEmail();
            htmlEmail.setCharset("UTF-8");
            htmlEmail.setHostName(this.senderEmail.getSmtpHost());
            htmlEmail.setAuthentication(this.senderEmail.getUserName(), this.senderEmail.getPassword());
            htmlEmail.setFrom(this.senderEmail.getFrom());
            if (this.senderEmail.isSsl()) {
                htmlEmail.setSSLOnConnect(true);
                htmlEmail.setSslSmtpPort(this.senderEmail.getSmtpPort().toString());
            } else {
                htmlEmail.setSmtpPort(this.senderEmail.getSmtpPort());
            }
            htmlEmail.setSubject(subject);
            htmlEmail.setHtmlMsg(html);
            htmlEmail.addTo(mails);
            htmlEmail.send();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private MailTemplate getMailTemplate(Application application) {
        long duration;
        if (application.getEndTime() == null) {
            duration = System.currentTimeMillis() - application.getStartTime().getTime();
        } else {
            duration = application.getEndTime().getTime() - application.getStartTime().getTime();
        }
        duration = duration / 1000;

        // TODO: modify url for both k8s and yarn execute mode, the k8s mode is different from yarn, when the flink job failed ,
        //  the k8s pod is missing , so we should look for  a more reasonable url for k8s
        String url = "";
        if (ExecutionMode.isYarnMode(application.getExecutionMode())) {
            String format = "%s/proxy/%s/";
            url = String.format(format, HadoopUtils.getRMWebAppURL(false), application.getAppId());
        }

        MailTemplate template = new MailTemplate();
        template.setJobName(application.getJobName());
        template.setLink(url);
        template.setStartTime(DateUtils.format(application.getStartTime(), DateUtils.fullFormat(), TimeZone.getDefault()));
        template.setEndTime(DateUtils.format(application.getEndTime() == null ? new Date() : application.getEndTime(), DateUtils.fullFormat(), TimeZone.getDefault()));
        template.setDuration(DateUtils.toRichTimeDuration(duration));
        boolean needRestart = application.isNeedRestartOnFailed() && application.getRestartCount() > 0;
        template.setRestart(needRestart);
        if (needRestart) {
            template.setRestartIndex(application.getRestartCount());
            template.setTotalRestart(application.getRestartSize());
        }
        return template;
    }

    /**
     * 发送飞书告警
     * @param mail 模板
     * @param subject 主题
     * @param fsWebhooks webhooks
     */
    private void sendFsMsg(MailTemplate mail, String subject,  String... fsWebhooks) {
        JsonNode body = getFsMdTemplate(mail, subject);
        for (String fsWebHook : fsWebhooks) {
            String result = null;
            try {
                result = HttpClientUtils.httpPostRequest(fsWebHook, body.toString());
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            System.out.println(result);
        }
    }

    /**
     * 参考：https://www.feishu.cn/hc/zh-CN/articles/360024984973
     * https://open.feishu.cn/tool/cardbuilder?from=custom_bot_doc
     * @param mail 模板
     * @param subject 主题
     * @return 飞书模板
     */
    private JsonNode getFsMdTemplate(MailTemplate mail, String subject){
        JsonMapper jsonMapper = JsonMapper.builder().build();

        String title = mail.getTitle();
        String jobName = mail.getJobName();
        String status = mail.getStatus();
        String startTime = mail.getStartTime();
        String endTime = mail.getEndTime();
        String duration = mail.getDuration();
        String link = mail.getLink();

        //header 封装：包含template 与 title
        ObjectNode header = jsonMapper.createObjectNode();
        header.put("template", "red");
        ObjectNode titleNode = jsonMapper.createObjectNode();
        titleNode.put("content", String.format("[Flink任务告警] %s", title));
        titleNode.put("tag", "plain_text");
        header.set("title", titleNode);

        //config 封装
        ObjectNode config = jsonMapper.createObjectNode();
        config.put("wide_screen_mode", true);

        //elements 封装：主要内容都在该部分
        ArrayNode elements = jsonMapper.createArrayNode();
        JsonNode jobNameNode = getLineTextModule("任务名称", jobName);
        JsonNode jobStatusNode = getLineTextModule("任务状态", status);
        JsonNode startTimeNode = getLineTextModule("启动时间", startTime);
        JsonNode endTimeNode = getLineTextModule("结束时间", endTime);
        JsonNode durationNode = getLineTextModule("运行时长", duration);
        JsonNode linkNode = getLineTextModule("查看日志", link);

        // 逐行添加
        elements.add(jobNameNode);
        elements.add(jobStatusNode);
        elements.add(startTimeNode);
        elements.add(endTimeNode);
        elements.add(durationNode);
        elements.add(linkNode);
        if (mail.getRestart()) {
            int restartIndex = mail.getRestartIndex();
            int totalRestart = mail.getTotalRestart();
            JsonNode restartNode = getLineTextModule("重启次数", String.format("%s/%s", restartIndex, totalRestart));
            elements.add(restartNode);
        }

        //封装信息脚
        ObjectNode hrNode = jsonMapper.createObjectNode();
        hrNode.put("tag", "hr");
        ObjectNode noteNode = jsonMapper.createObjectNode();
        ArrayNode noteElements = jsonMapper.createArrayNode();
        ObjectNode noteContent = jsonMapper.createObjectNode();
        noteContent.put("content", "[来自StreamX](http://172.16.244.42:10000/)");
        noteContent.put("tag", "lark_md");
        noteElements.add(noteContent);
        noteNode.set("elements", noteElements);
        noteNode.put("tag", "note");
        // 最后增加分割线与note
        elements.add(hrNode);
        elements.add(noteNode);

        //card 封装：包括config, elements, header 三个部分
        ObjectNode card = jsonMapper.createObjectNode();
        card.set("config", config);
        card.set("elements", elements);
        card.set("header", header);

        //消息体：包含msg_type与card
        ObjectNode body = jsonMapper.createObjectNode();
        body.put("msg_type", "interactive");
        body.set("card", card);
        System.out.println(body);
        return body;
    }

    /**
     * 生成单行的模板
     * @param title 标题
     * @param content 内容
     * @return 一行
     */
    private JsonNode getLineTextModule(String title, String content){
        JsonMapper jsonMapper = JsonMapper.builder().build();
        ObjectNode line = jsonMapper.createObjectNode();
        line.put("tag", "div");
        ArrayNode line1Fields = jsonMapper.createArrayNode();
        ObjectNode jobNameNode = jsonMapper.createObjectNode();
        jobNameNode.put("is_short", true);
        ObjectNode jobNameText = jsonMapper.createObjectNode();
        jobNameText.put("content", String.format("**%s:** %s", title, content));
        jobNameText.put("tag", "lark_md");
        jobNameNode.set("text", jobNameText);
        line1Fields.add(jobNameNode);
        line.set("fields", line1Fields);
        return line;
    }

}
