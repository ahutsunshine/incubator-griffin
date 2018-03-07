/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package org.apache.griffin.core.exception;

public enum GriffinExceptionMessage {

    //400, "Bad Request"
    MEASURE_TYPE_DOES_NOT_MATCH(40001, "Property 'measure.type' does not match the type of measure in request body"),
    INVALID_CONNECTOR_NAME(40002, "Property 'name' in 'connectors' field is invalid"),
    MISSING_METRIC_NAME(40003, "Missing property 'metricName'"),
    INVALID_JOB_NAME(40004, "Property 'job.name' is invalid"),
    MISSING_BASELINE_CONFIG(40005, "Missing 'as.baseline' config in 'data.segments'"),
    INVALID_METRIC_RECORDS_OFFSET(40006, "Offset must not be less than zero"),
    INVALID_METRIC_RECORDS_SIZE(40007, "Size must not be less than zero"),
    INVALID_METRIC_VALUE_FORMAT(40008, "Metric value format is invalid"),
    INVALID_MEASURE_ID(40009, "Property 'measure.id' is invalid"),
    INVALID_CRON_EXPRESSION(40010, "Property 'cron.expression' is invalid"),
    HIVE_DATA_ASSET_CANNOT_UPDATED(40011, "Hive data asset can not be updated"),

    //404, "Not Found"
    MEASURE_ID_DOES_NOT_EXIST(40401, "Measure id does not exist"),
    JOB_ID_DOES_NOT_EXIST(40402, "Job id does not exist"),
    JOB_NAME_DOES_NOT_EXIST(40403, "Job name does not exist"),
    DATA_ASSET_DOES_NOT_FOUND(40404,"Data asset does not exist"),

    //409, "Conflict"
    MEASURE_NAME_ALREADY_EXIST(40901, "Measure name already exists"),
    HIVE_DATA_ASSET_ALREADY_EXIST(40901, "Hive data asset already exists"),
    QUARTZ_JOB_ALREADY_EXIST(40902, "Quartz job already exist");

    private final int code;
    private final String message;

    GriffinExceptionMessage(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public static GriffinExceptionMessage valueOf(int code) {
        GriffinExceptionMessage[] messages = values();
        int len = values().length;
        for (int i = 0; i < len; i++) {
            GriffinExceptionMessage message = messages[i];
            if (message.code == code) {
                return message;
            }
        }
        throw new IllegalArgumentException("No matching constant for [" + code + "]");
    }


    @Override
    public String toString() {
        return Integer.toString(code);
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
