package com.krad.google;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;

import com.google.cloud.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class TryIt {
    private static final Logger LOGGER = LoggerFactory.getLogger(TryIt.class);

    public static void main(String [] args) {

        String path = args[0];
        URI uri = URI.create(path);
        String bucketName = uri.getHost();

        try {
            LOGGER.info(String.format("Bucket to be created is %s", bucketName));
            Bucket logBucket = new StorageUtils().createBucket(uri.getHost());

        } catch (StorageException e) {
            if (e.getCode() == 409) {
                LOGGER.warn(e.getCode()+" You already have that bucket. Proceed.", e.getCode());
            } else {
                LOGGER.error(e.getCode()+" "+e.getMessage(), e.getCode());
            }
        }


        String directory = uri.getPath();
        if (directory != null && directory.length() > 0) {
            directory = directory.substring(1, directory.length());
        }
        LOGGER.info(String.format("Directory to be created %s", directory));
        Blob intermediateDir = new StorageUtils().createBlob(bucketName, directory+"/");
        int intermediateDirRS = new StorageUtils().getFileCount(bucketName, directory+"/");

        LOGGER.info(String.format("Directory current size is: %s", intermediateDirRS));
    }

}
