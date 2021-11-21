# aws-zarr

The aws-zarr project provides an implementation of a com.bc.zarr.storage.Store
that is backed by an AWS S3 bucket


Additional project information, javadocs, and test coverage is located at https://ci-cmg.github.io/project-documentation/aws-zarr/

## Adding To Your Project

Add the following dependency to your Maven pom.xml

```xml
    <dependency>
      <groupId>io.github.ci-cmg</groupId>
      <artifactId>aws-zarr</artifactId>
      <version>1.0.0-SNAPSHOT</version>
    </dependency>
```
You will also need a version of JZarr

```xml
    <dependency>
      <groupId>com.bc.zarr</groupId>
      <artifactId>jzarr</artifactId>
      <version>0.3.4</version>
    </dependency>
```

## Usage

The minimal way to create a AwsS3ZarrStore is as follows:
```java
Store store = AwsS3ZarrStore.builder()
    .s3(s3)
    .bucket(bucketName)
    .key(key)
    .build();
```

Where in the above example, s3 is an instance of S3ClientWrapper (described below), bucketName is the name of
a S3 bucket, and key is the key representing the root of a zarr store.

Here is how to create a S3OutputStream with all the available options:
```java
Store store = AwsS3ZarrStore.builder()
    .s3(s3)
    .bucket(bucketName)
    .key(key)
    .multipartUploadMb(multipartUploadMb)
    .maxUploadBuffers(maxUploadBuffers)
    .build();
```

This project uses a S3OutputStream to upload files in parts. multipartUploadMb represents the size of the parts to
upload in MiB.  This value must be at least 5, which is the default.

A S3OutputStream uses a queue to allow multipart uploads to S3 to happen while additional
buffers are being filled concurrently. The maxUploadBuffers defines the number of parts
to be queued before blocking population of additional parts.  The default value is 1.
Specifying a higher value may improve upload speed at the expense of more heap usage.
Using a value higher than one should be tested to see if any performance gains are achieved
for your situation.

## S3ClientWrapper
s3 is an instance of S3ClientWrapper.  The S3ClientWrapper is a wrapper
around the S3Client from the AWS SDK v2.  This allows for calls to the S3Client to
be mocked for testing.  Two implementations are provided:

1. AwsS3ClientWrapper - This uses the S3Client to make calls using the AWS SDK.
2. FileMockS3ClientWrapper - This reads and writes from the local file system. This should only be used for testing.

An instance of AwsS3ClientWrapper can be created as follows:
```java
AwsS3ClientWrapper s3 = AwsS3ClientWrapper.builder()
    .s3(s3)
    .build();
```