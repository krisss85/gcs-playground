package com.krad.google;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class StorageUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageUtils.class);
    private static final Storage storage = StorageOptions.getDefaultInstance().getService();

    public Bucket checkBucket(String bucketName) {
        if (bucketName == null) {
            throw new IllegalArgumentException(
                    "Provide bucket name.");
        } else {
            return storage.get(bucketName, Storage.BucketGetOption.fields());
        }
    }

    public Bucket createBucket(String bucketName) {
        Bucket bucket = storage.create(BucketInfo.newBuilder(bucketName)
                .setLocation("europe-west1")
                .build());
        return bucket;
    }

    public Blob createBlob(String bucketName, String blobName) {
        BlobId blobId = BlobId.of(bucketName, blobName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        Blob blob = storage.create(blobInfo);
        return blob;
    }

    public Page<Blob> listBucket(String bucketName, String directory) {
        if (bucketName == null) {
            throw new IllegalArgumentException(
                    "Provide bucket name.");
        } else {
            Bucket bucket = storage.get(bucketName);
            if (bucket == null) {
                throw new IllegalArgumentException(
                        "No such bucket.");
            }
            Page<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(directory));
            return blobs;
        }
    }

    public int getFileCount(String bucketName, String directory) {
        Iterator bucketIterator = listBucket(bucketName, directory).getValues().iterator();
        int count = 0;
        while (bucketIterator.hasNext()) {
            bucketIterator.next();
            count++;
        }
        return count;
    }
}
