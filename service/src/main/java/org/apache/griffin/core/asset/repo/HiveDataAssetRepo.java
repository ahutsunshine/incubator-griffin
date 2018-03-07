package org.apache.griffin.core.asset.repo;

import org.apache.griffin.core.asset.entity.HiveDataAsset;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface HiveDataAssetRepo extends DataAssetRepo<HiveDataAsset> {

    @Transactional(rollbackFor = Exception.class)
    @Modifying
    @Query("delete from HiveDataAsset hda where hda.modifiedDate < ?1")
    int deleteByModifiedDate(Long latestTms);

    @Query("select hda from HiveDataAsset hda")
    List<HiveDataAsset> findAll(Pageable pageable);
}
