package org.apache.griffin.core.asset;

import org.apache.griffin.core.asset.entity.DataAsset;
import org.apache.griffin.core.asset.entity.HiveDataAsset;
import org.apache.griffin.core.asset.repo.DataAssetRepo;
import org.apache.griffin.core.exception.GriffinException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.apache.griffin.core.exception.GriffinExceptionMessage.HIVE_DATA_ASSET_ALREADY_EXIST;
import static org.apache.griffin.core.exception.GriffinExceptionMessage.HIVE_DATA_ASSET_CANNOT_UPDATED;

@Component("hiveOperation")
public class HiveDataAssetOperationImpl implements DataAssetOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveDataAssetOperationImpl.class);
    @Autowired
    private DataAssetRepo<DataAsset> dataAssetRepo;

    @Override
    public DataAsset create(DataAsset dataAsset) {
        HiveDataAsset asset = (HiveDataAsset) dataAsset;
        String id = asset.getDbName() + "_" + asset.getTableName() + "_" + String.valueOf(false);
        if (dataAssetRepo.findOne(id) != null) {
            LOGGER.warn("dbName:{} tableName:{} registerFromHive:false have already existed.",asset.getDbName(),asset.getTableName());
            throw new GriffinException.ConflictException(HIVE_DATA_ASSET_ALREADY_EXIST);
        }
        asset.setRegisterFromHive(false);
        asset.setId(id);
        return dataAssetRepo.save(asset);
    }

    @Override
    public void delete(DataAsset dataAsset) {
        dataAssetRepo.delete(dataAsset.getId());
    }

    @Override
    public void update(DataAsset dataAsset) {
        HiveDataAsset asset = (HiveDataAsset) dataAsset;
        if (asset.isRegisterFromHive()) {
            LOGGER.warn("Id {} belongs to hive data asset type.Hive data asset can not be updated.", dataAsset.getId());
            throw new GriffinException.BadRequestException(HIVE_DATA_ASSET_CANNOT_UPDATED);
        }
        dataAssetRepo.save(asset);
    }
}
