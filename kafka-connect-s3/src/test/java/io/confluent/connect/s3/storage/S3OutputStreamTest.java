package io.confluent.connect.s3.storage;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.SkipMd5CheckStrategy;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.S3SinkConnectorTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class S3OutputStreamTest extends S3SinkConnectorTestBase {

  AmazonS3 realS3;

  @Before
  public void setup() throws Exception {
    super.setUp();
    url = null;
    realS3 = newS3Client(connectorConfig);
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(S3SinkConnectorConfig.REGION_CONFIG, "us-east-1");
    props.put(S3SinkConnectorConfig.PART_SIZE_CONFIG, String.valueOf(25 * 1024 * 1024));
    return props;
  }

  @Test
  public void testWriteToRealS3() throws Exception {
    S3OutputStream stream = new S3OutputStream(
        "s3-output-stream-test", connectorConfig, realS3);
    stream.write(new byte[]{'a', 'b', 'c', 'd'});
    stream.commit();
  }

  @Test
  public void testWriteBigFileToRealS3() throws Exception {
    int size = 75 * 1024 * 1024;  // 75 MB should trigger a 3-part multipart
    byte[] content = new byte[size];
    Arrays.fill(content, (byte) 'R');

    S3OutputStream stream = new S3OutputStream(
        "s3-output-stream-big-file-multipart-test", connectorConfig, realS3);
    stream.write(content);
    stream.commit();
  }
}
