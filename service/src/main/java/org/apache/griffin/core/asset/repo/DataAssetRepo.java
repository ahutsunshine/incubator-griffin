package org.apache.griffin.core.asset.repo;

import org.apache.griffin.core.asset.entity.DataAsset;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface DataAssetRepo<T extends DataAsset> extends CrudRepository<T, String> {

    @Query("select da from  #{#entityName} da")
    List<T> findAll(Pageable pageable);
}
