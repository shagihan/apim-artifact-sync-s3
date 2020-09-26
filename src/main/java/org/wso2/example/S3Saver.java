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
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.wso2.carbon.apimgt.impl.APIConstants;
import org.wso2.carbon.apimgt.impl.APIManagerConfiguration;
import org.wso2.carbon.apimgt.impl.dto.Environment;
import org.wso2.carbon.apimgt.impl.gatewayartifactsynchronizer.ArtifactSaver;
import org.wso2.carbon.apimgt.impl.gatewayartifactsynchronizer.exception.ArtifactSynchronizerException;
import org.wso2.carbon.apimgt.impl.internal.ServiceReferenceHolder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class S3Saver implements ArtifactSaver {

    private static final Log log = LogFactory.getLog(S3Saver.class);
    private static String DB_SAVER_NAME = "S3Saver";
    private static String DEFAULT_CONTENT_TYPE = "application/json";
    AWSUtils awsUtils;

    public void init() throws ArtifactSynchronizerException {
        String accessKey = System.getProperty("accessKey");
        String secretKey = System.getProperty("secretKey");
        String awsRegion = System.getProperty("awsRegion");
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(awsRegion)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
        awsUtils = new AWSUtils(s3Client);
    }

    public void saveArtifact(String gatewayRuntimeArtifacts, String gatewayLabel, String gatewayInstruction)
            throws ArtifactSynchronizerException {
        JSONObject artifactObject = new JSONObject(gatewayRuntimeArtifacts);
        String apiId = (String) artifactObject.get("apiId");
        String apiName = (String) artifactObject.get("name");
        String version = (String) artifactObject.get("version");
        String tenantDomain = (String) artifactObject.get("tenantDomain");
        String bucketName = gatewayLabel.trim().replaceAll("\\s","").toLowerCase();
        String fileName = apiId + ".json";
        awsUtils.createBucketIfNotExist(bucketName);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.addUserMetadata("apiName", apiName);
        metadata.addUserMetadata("version", version);
        metadata.addUserMetadata("tenantDomain", tenantDomain);
        metadata.addUserMetadata("apiId", apiId);
        metadata.addUserMetadata("gatewayInstruction", gatewayInstruction);
        metadata.addUserMetadata("gatewayEnv", gatewayLabel);
        metadata.setContentType(DEFAULT_CONTENT_TYPE);
        InputStream gatewayRuntimeArtifactsStream = new
                ByteArrayInputStream(gatewayRuntimeArtifacts.getBytes(StandardCharsets.UTF_8));
        awsUtils.putAFile(bucketName, fileName, gatewayRuntimeArtifactsStream, metadata);
    }

    public boolean isAPIPublished(String s) {
        APIManagerConfiguration apiManagerConfiguration =
                ServiceReferenceHolder.getInstance().getAPIManagerConfigurationService().getAPIManagerConfiguration();
        Map<String, Environment> gatewayEnvs = apiManagerConfiguration.getApiGatewayEnvironments();
        for (String eachEnv : gatewayEnvs.keySet()) {
            String bucketName = eachEnv.trim().replaceAll("\\s","").toLowerCase();
            try {
                if (awsUtils.isPublishedInGateway(bucketName, s + ".json",
                        APIConstants.GatewayArtifactSynchronizer.GATEWAY_INSTRUCTION_PUBLISH)) {
                    return true;
                }
            } catch (ArtifactSynchronizerException e) {
                log.error(e.getMessage(), e);
            }
        }
        return false;
    }

    public void disconnect() {

    }

    public String getName() {
        return DB_SAVER_NAME;
    }
}
