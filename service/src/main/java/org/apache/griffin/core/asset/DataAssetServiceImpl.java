package org.apache.griffin.core.asset;

import org.apache.griffin.core.asset.entity.DataAsset;
import org.apache.griffin.core.asset.entity.HiveDataAsset;
import org.apache.griffin.core.asset.repo.DataAssetRepo;
import org.apache.griffin.core.asset.repo.HiveDataAssetRepo;
import org.apache.griffin.core.exception.GriffinException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import static org.apache.griffin.core.exception.GriffinExceptionMessage.DATA_ASSET_DOES_NOT_FOUND;

@Service
public class DataAssetServiceImpl implements DataAssetService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataAssetServiceImpl.class);
    private static final int MAX_PAGE_SIZE = 1024;
    private static final String HIVE_TYPE = "hive";

    @Autowired
    private DataAssetRepo<DataAsset> dataAssetRepo;

    @Autowired
    private HiveDataAssetRepo hiveDataAssetRepo;

    @Autowired
    @Qualifier("hiveOperation")
    private DataAssetOperation hiveOp;

    @Override
    public DataAsset createDataAsset(DataAsset dataAsset) {
        DataAssetOperation op = getOperation(dataAsset);
        return op.create(dataAsset);
    }

    @Override
    public void deleteDataAsset(String id) {
        DataAsset dataAsset = dataAssetRepo.findOne(id);
        if (dataAsset == null) {
            throw new GriffinException.NotFoundException(DATA_ASSET_DOES_NOT_FOUND);
        }
        DataAssetOperation op = getOperation(dataAsset);
        op.delete(dataAsset);
    }

    @Override
    public void updateDataAsset(DataAsset dataAsset) {
        DataAsset asset = dataAssetRepo.findOne(dataAsset.getId());
        if (asset == null) {
            throw new GriffinException.NotFoundException(DATA_ASSET_DOES_NOT_FOUND);
        }
        DataAssetOperation op = getOperation(dataAsset);
        op.update(dataAsset);
    }

    @Override
    public Iterable<? extends DataAsset> getDataAssets(String type, int page, int size) {
        size = size > MAX_PAGE_SIZE ? MAX_PAGE_SIZE : size;
        Pageable pageable = new PageRequest(page, size, Sort.Direction.DESC, "createdDate");
        if (type.equals(HIVE_TYPE)) {
            return hiveDataAssetRepo.findAll(pageable);
        }
        return dataAssetRepo.findAll(pageable);
    }

    private DataAssetOperation getOperation(DataAsset dataAsset) {
        if (dataAsset instanceof HiveDataAsset) {
            return hiveOp;
        }
        throw new IllegalArgumentException("No such data asset type.");
    }
}
