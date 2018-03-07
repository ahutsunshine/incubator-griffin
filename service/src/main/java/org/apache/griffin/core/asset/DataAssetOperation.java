package org.apache.griffin.core.asset;

import org.apache.griffin.core.asset.entity.DataAsset;

public interface DataAssetOperation {
    DataAsset create(DataAsset dataAsset);

    void delete(DataAsset dataAsset);

    void update(DataAsset dataAsset);
}
