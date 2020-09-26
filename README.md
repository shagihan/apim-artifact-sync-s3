# Configure S3 as artifact synchronize mechanism for WSO2 API Manager

With the WSO2 API Manager 3.2.0 onwards It's introduced a 3rd option to sync artifacts between gateways. In previous versions it supported only
- Shared file system (e.g., NFS)
- Rsync

Now we have a extensible Inbuilt artifact synchronizer to sync artifacts. This is a sample implementation to share artifacts using the Amazon S3 service. 

## Getting Started

To get started, go to [WSO2 APIM 3.2.0 Sync Synapse Artifacts using S3 bucket]().

## Build

APIM 3.x version related code can be found in the master branch.

Use the following command to build this implementation
`mvn clean install`


