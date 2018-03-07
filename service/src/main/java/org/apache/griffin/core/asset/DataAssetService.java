package org.apache.griffin.core.asset;

import org.apache.griffin.core.asset.entity.DataAsset;
import org.apache.griffin.core.asset.entity.HiveDataAsset;

public interface DataAssetService {
    DataAsset createDataAsset(DataAsset dataAsset);

    void deleteDataAsset(String id);

    void updateDataAsset(DataAsset dataAsset);

    Iterable<? extends DataAsset> getDataAssets(String type,int page, int size);
}
