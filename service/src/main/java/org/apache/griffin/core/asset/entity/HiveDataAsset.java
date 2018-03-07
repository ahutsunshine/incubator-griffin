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
package org.apache.griffin.core.asset.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.util.JsonUtil;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Entity
public class HiveDataAsset extends DataAsset {
    @NotNull
    private String dbName;

    @NotNull
    private String tableName;

    @NotNull
    private boolean registerFromHive = true;

    @NotNull
    private String location;

    @JsonIgnore
    @Access(AccessType.PROPERTY)
    @Column(length = 4*1024)
    private String sd;

    @Transient
    private Map<String, Object> sdMap;

    private String owner;

    public HiveDataAsset() {
    }

    public HiveDataAsset(String id, String dbName, String tableName, DataAssetType type, String version, String location, Map<String, Object> sdMap) {
        super(id, type, version);
        this.dbName = dbName;
        this.tableName = tableName;
        this.location = location;
        this.sdMap = sdMap;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isRegisterFromHive() {
        return registerFromHive;
    }

    public void setRegisterFromHive(boolean registerFromHive) {
        this.registerFromHive = registerFromHive;
    }

    public String getSd() {
        return sd;
    }

    public void setSd(String sd) throws IOException {
        if (!StringUtils.isEmpty(sd)) {
            this.sd = sd;
            this.sdMap = JsonUtil.toEntity(sd, new TypeReference<Map<String, List<PartitionKey>>>() {
            });
        }
    }

    @JsonProperty("sd")
    public Map<String, Object> getSdMap() {
        return sdMap;
    }

    @JsonProperty("sd")
    public void setSdMap(Map<String, Object> sdMap) throws JsonProcessingException {
        this.sdMap = sdMap;
        this.sd = JsonUtil.toJson(sdMap);
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }
}

class PartitionKey {
    public String name;
    public String type;
    public String comment;
    public Boolean setComment;
    public Boolean setType;
    public Boolean setName;

}
