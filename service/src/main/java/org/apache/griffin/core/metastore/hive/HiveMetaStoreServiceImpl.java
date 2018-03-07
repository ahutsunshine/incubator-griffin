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

package org.apache.griffin.core.metastore.hive;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.griffin.core.asset.entity.DataAsset;
import org.apache.griffin.core.asset.entity.HiveDataAsset;
import org.apache.griffin.core.asset.repo.DataAssetRepo;
import org.apache.griffin.core.asset.repo.HiveDataAssetRepo;
import org.apache.griffin.core.util.JsonUtil;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Service
@CacheConfig(cacheNames = "hive", keyGenerator = "cacheKeyGenerator")
public class HiveMetaStoreServiceImpl implements HiveMetaStoreService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveMetaStoreService.class);
    private static Long dataAssetLatestTime;

    @Autowired
    private HiveMetaStoreClient client;

    @Autowired
    private HiveDataAssetRepo hiveDataAssetRepo;

    @Value("${hive.metastore.filter.dbname}")
    private String filterDatabase;

    private ThreadPoolExecutor singleThreadExecutor;

    public HiveMetaStoreServiceImpl() {
        singleThreadExecutor = new ThreadPoolExecutor(1, 5, 3, TimeUnit.SECONDS, new ArrayBlockingQueue<>(3), new ThreadPoolExecutor.DiscardPolicy());
        LOGGER.info("HiveMetaStoreServiceImpl single thread pool created.");
    }

    @Override
    @Cacheable
    public Iterable<String> getAllDatabases() {
        Iterable<String> results = null;
        try {
            if (client == null) {
                LOGGER.warn("Hive client is null. Please check your hive config.");
                return new ArrayList<>();
            }
            results = client.getAllDatabases();
        } catch (MetaException e) {
            reconnect();
            LOGGER.error("Can not get databases : {}", e.getMessage());
        }
        return results;
    }


    @Override
    @Cacheable
    public Iterable<String> getAllTableNames(String dbName) {
        Iterable<String> results = null;
        try {
            if (client == null) {
                LOGGER.warn("Hive client is null. Please check your hive config.");
                return new ArrayList<>();
            }
            results = client.getAllTables(getUseDbName(dbName));
        } catch (Exception e) {
            reconnect();
            LOGGER.error("Exception fetching tables info: {}", e.getMessage());
        }
        return results;
    }


    @Override
    @Cacheable
    public List<Table> getAllTable(String db) {
        return getTables(db);
    }

    @Override
    @Cacheable
    @Transactional
    public Map<String, List<Table>> getFilterTables() throws IOException {
        Map<String, List<Table>> results = new HashMap<>();
        Iterable<String> dbs;
        // if hive.metastore.uris in application.properties configs wrong, client will be injected failure and will be null.
        if (client == null) {
            LOGGER.warn("Hive client is null. Please check your hive config.");
            return results;
        }
        if (StringUtils.isEmpty(filterDatabase)) {
            dbs = getAllDatabases();
            results = getAllTables(dbs);
        } else {
            String[] filterDbs = filterDatabase.split(",");
            results = getAllTables(Arrays.asList(filterDbs));
        }
        List<HiveDataAsset> assets = getDataAssetFromHive(results);
        hiveDataAssetRepo.save(assets);
        hiveDataAssetRepo.deleteByModifiedDate(dataAssetLatestTime);
        return results;
    }


    @Override
    @Cacheable
    public Table getTable(String dbName, String tableName) {
        Table result = null;
        try {
            if (client == null) {
                LOGGER.warn("Hive client is null. Please check your hive config.");
                return null;
            }
            result = client.getTable(getUseDbName(dbName), tableName);
        } catch (Exception e) {
            reconnect();
            LOGGER.error("Exception fetching table info : {}. {}", tableName, e.getMessage());
        }
        return result;
    }

    private List<HiveDataAsset> getDataAssetFromHive(Map<String, List<Table>> results) throws IOException {
        List<HiveDataAsset> assets = new ArrayList<>();
        dataAssetLatestTime = System.currentTimeMillis();
        for (Map.Entry<String, List<Table>> entry : results.entrySet()) {
            List<Table> tables = entry.getValue();
            for (Table table : tables) {
                assets.add(genHiveDataAsset(table));
            }
        }
        return assets;
    }

    private HiveDataAsset genHiveDataAsset(Table table) throws JsonProcessingException {
        HiveDataAsset asset = new HiveDataAsset();
        asset.setType(DataAsset.DataAssetType.HIVE);
        asset.setVersion("1.2");
        asset.setDbName(table.getDbName());
        asset.setTableName(table.getTableName());
        Map<String, Object> sdMap = new HashMap<>();
        sdMap.put("partitionKeys",table.getPartitionKeys());
        sdMap.put("cols",table.getSd().getCols());
        asset.setSdMap(sdMap);
        asset.setRegisterFromHive(true);
        asset.setLocation(table.getSd().getLocation());
        asset.setId(asset.getDbName() + "_" + asset.getTableName() + "_" + String.valueOf(true));
        return asset;
    }

    private Map<String, List<Table>> getAllTables(Iterable<String> dbs) {
        Map<String, List<Table>> results = new HashMap<>();
        if (dbs == null) {
            return results;
        }
        for (String db : dbs) {
            results.put(db, getTables(db));
        }
        return results;
    }

    private List<Table> getTables(String db) {
        String useDbName = getUseDbName(db);
        List<Table> allTables = new ArrayList<>();
        try {
            if (client == null) {
                LOGGER.warn("Hive client is null. Please check your hive config.");
                return allTables;
            }
            Iterable<String> tables = client.getAllTables(useDbName);
            for (String table : tables) {
                Table tmp = client.getTable(db, table);
                allTables.add(tmp);
            }
        } catch (Exception e) {
            reconnect();
            LOGGER.error("Exception fetching tables info: {}", e.getMessage());
        }
        return allTables;
    }

    private String getUseDbName(String dbName) {
        if (StringUtils.isEmpty(dbName)) {
            return "default";
        } else {
            return dbName;
        }
    }

    private void reconnect() {
        if (singleThreadExecutor.getActiveCount() == 0) {
            System.out.println("execute create thread.");
            singleThreadExecutor.execute(() -> {
                try {
                    client.reconnect();
                } catch (MetaException e) {
                    LOGGER.error("reconnect to hive failed.");
                }
            });
        }
    }
}
