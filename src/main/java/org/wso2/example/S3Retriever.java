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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.apimgt.impl.APIConstants;
import org.wso2.carbon.apimgt.impl.gatewayartifactsynchronizer.ArtifactRetriever;
import org.wso2.carbon.apimgt.impl.gatewayartifactsynchronizer.exception.ArtifactSynchronizerException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class S3Retriever implements ArtifactRetriever {

    private static final Log log = LogFactory.getLog(S3Retriever.class);
    private static String DB_RETRIEVER_NAME = "S3Retriever";
    AWSUtils awsUtils;

    @Override
    public void init() throws ArtifactSynchronizerException {
        String accessKey = System.getProperty("accessKey");
        String secretKey = System.getProperty("secretKey");
        String awsRegion = System.getProperty("awsRegion");
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(awsRegion)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
        awsUtils = new AWSUtils(s3Client);
    }

    @Override
    public String retrieveArtifact(String APIId, String gatewayLabel, String gatewayInstruction)
            throws ArtifactSynchronizerException, IOException {
        String bucketName = gatewayLabel.trim().replaceAll("\\s", "").toLowerCase();
        String fileName = APIId + ".json";
        return awsUtils.getFile(bucketName, fileName, gatewayInstruction);
    }

    @Override
    public Map<String, String> retrieveAttributes(String apiName, String version, String tenantDomain)
            throws ArtifactSynchronizerException {
        Map<String, String> apiAttributes = null;
        List<S3Object> apiList = awsUtils.getAllAPIs();
        for (S3Object api : apiList) {
            String spiName = api.getObjectMetadata().getUserMetadata().get("apiName");
            String artifactVersion = api.getObjectMetadata().getUserMetadata().get("version");
            String artifactTenantDomain = api.getObjectMetadata().getUserMetadata().get("tenantDomain");
            if (apiName.equals(spiName) && version.equals(artifactVersion)
                    && tenantDomain.equals(artifactTenantDomain)) {
                String apiId = api.getObjectMetadata().getUserMetadata().get("apiId");
                String label = api.getObjectMetadata().getUserMetadata().get("gatewayEnv");
                apiAttributes.put(APIConstants.GatewayArtifactSynchronizer.API_ID, apiId);
                apiAttributes.put(APIConstants.GatewayArtifactSynchronizer.LABEL, label);
            }
        }
        return apiAttributes;
    }

    @Override
    public List<String> retrieveAllArtifacts(String gatewayLabel) throws ArtifactSynchronizerException, IOException {
        String bucketName = gatewayLabel.trim().replaceAll("\\s", "").toLowerCase();
        return awsUtils.getListOfAPIs(bucketName);
    }

    @Override
    public void disconnect() {

    }

    @Override
    public String getName() {
        return DB_RETRIEVER_NAME;
    }
}
