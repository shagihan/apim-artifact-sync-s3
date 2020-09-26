/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.example;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.apimgt.impl.APIConstants;
import org.wso2.carbon.apimgt.impl.APIManagerConfiguration;
import org.wso2.carbon.apimgt.impl.gatewayartifactsynchronizer.exception.ArtifactSynchronizerException;
import org.wso2.carbon.apimgt.impl.internal.ServiceReferenceHolder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AWSUtils {

    private static final Log log = LogFactory.getLog(AWSUtils.class);
    private static final String API_STATUS_KEY = "api-status";
    private AmazonS3 s3Client;

    public AWSUtils(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    public String getFile(String bucketName, String fileObjKeyName, String gatewayInstruction)
            throws ArtifactSynchronizerException {
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, fileObjKeyName);
            S3Object object = s3Client.getObject(request);
            if (object != null && (object.getObjectMetadata().getUserMetadata().get("gatewayInstruction")
                    .equals(gatewayInstruction) ||
                    APIConstants.GatewayArtifactSynchronizer.GATEWAY_INSTRUCTION_ANY.equals(gatewayInstruction))) {
                InputStream inputStream = object.getObjectContent();
                return getTextFromInputStream(inputStream);
            } else {
                return "";
            }
        } catch (AmazonServiceException e) {
            throw new ArtifactSynchronizerException(e.getMessage(), e);
        } catch (SdkClientException e) {
            throw new ArtifactSynchronizerException(e.getMessage(), e);
        } catch (IOException e) {
            throw new ArtifactSynchronizerException("Error while reading S3 response.", e);
        }
    }

    public List<String> getListOfAPIs(String bucket) throws IOException {
        List<String> list = new ArrayList<>();
        try {
            ObjectListing listing = s3Client.listObjects(bucket);
            List<S3ObjectSummary> summaries = listing.getObjectSummaries();
            for (S3ObjectSummary key : summaries) {
                String file = key.getKey();
                S3Object object = s3Client.getObject(bucket, file);
                if (object != null) {
                    list.add(getTextFromInputStream(object.getObjectContent()));
                }
            }
        } catch (AmazonServiceException e) {
            log.error(e.getMessage(), e);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return list;
    }

    public void putAFile(String bucketName, String fileObjKeyName, InputStream input, ObjectMetadata metadata)
            throws ArtifactSynchronizerException {
        try {
            PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, input, metadata);
            s3Client.putObject(request);
            log.debug("Object : " + fileObjKeyName + "Successfully created in " + bucketName);
        } catch (AmazonServiceException e) {
            throw new ArtifactSynchronizerException(e.getMessage(), e);
        } catch (SdkClientException e) {
            throw new ArtifactSynchronizerException(e.getMessage(), e);
        }
    }

    public void createBucketIfNotExist(String bucket) throws ArtifactSynchronizerException {
        CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucket);
        try {
            s3Client.createBucket(createBucketRequest);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 409) {
                log.warn("Bucket already exist.");
            } else {
                throw new ArtifactSynchronizerException(e.getMessage(), e);
            }
        }
    }

    private String getTextFromInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        StringBuilder sb = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        return sb.toString();
    }

    public boolean isPublishedInGateway(String bucketName, String fileObjKeyName, String status)
            throws ArtifactSynchronizerException {
        GetObjectRequest request = new GetObjectRequest(bucketName, fileObjKeyName);
        try {
            S3Object object = s3Client.getObject(request);
            if (object != null) {
                String gatewayInstruction = object.getObjectMetadata().getUserMetadata().get("gatewayInstruction");
                if (gatewayInstruction.equals(status)) {
                    return true;
                }
            }
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                log.warn("Requested file " + fileObjKeyName + " not exist in bucket : " + bucketName);
            } else if (e.getStatusCode() == 400) {
                log.warn("Requested bucket " + bucketName + " not exist.");
            } else {
                throw new ArtifactSynchronizerException(e.getMessage(), e);
            }
        }
        return false;
    }

    public List<S3Object> getAllAPIs() {
        APIManagerConfiguration apiManagerConfiguration = ServiceReferenceHolder.getInstance()
                .getAPIManagerConfigurationService().getAPIManagerConfiguration();

        List<S3ObjectSummary> fullS3ObjectSummarylist = new ArrayList<>();
        for (String key : apiManagerConfiguration.getApiGatewayEnvironments().keySet()) {
            String bucket = key.trim().replaceAll("\\s", "").toLowerCase();
            ObjectListing listing = s3Client.listObjects(bucket);
            List<S3ObjectSummary> summaries = listing.getObjectSummaries();
            fullS3ObjectSummarylist = Stream.concat(fullS3ObjectSummarylist.stream(), summaries.stream())
                    .collect(Collectors.toList());
        }
        List<S3Object> objectList = new ArrayList<>();
        for (S3ObjectSummary summary : fullS3ObjectSummarylist) {
            String file = summary.getKey();
            S3Object object = s3Client.getObject(summary.getBucketName(), file);
            objectList.add(object);
        }
        return objectList;
    }
}
