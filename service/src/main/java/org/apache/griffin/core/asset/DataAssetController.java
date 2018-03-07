package org.apache.griffin.core.asset;

import org.apache.griffin.core.asset.entity.DataAsset;
import org.apache.griffin.core.interceptor.Token;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/api/v1")
public class DataAssetController {
    @Autowired
    private DataAssetService dataAssetService;

    @RequestMapping(value = "/assets", method = RequestMethod.GET)
    public Iterable<? extends DataAsset> getDataAssets(@RequestParam(value = "type", defaultValue = "") String type, @RequestParam("page") int page, @RequestParam("size") int size) {
        return dataAssetService.getDataAssets(type, page, size);
    }

    @RequestMapping(value = "/assets", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.CREATED)
    @Token
    public DataAsset createDataAssets(@RequestBody DataAsset dataAsset) {
        return dataAssetService.createDataAsset(dataAsset);
    }

    @RequestMapping(value = "/assets", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteDataAsset(@PathVariable String id) {
        dataAssetService.deleteDataAsset(id);
    }

    @RequestMapping(value = "/assets", method = RequestMethod.PUT)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateDataAsset(@RequestBody DataAsset dataAsset) {
        dataAssetService.updateDataAsset(dataAsset);
    }

}
