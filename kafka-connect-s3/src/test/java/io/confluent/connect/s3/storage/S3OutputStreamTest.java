package io.confluent.connect.s3.storage;

import com.amazonaws.services.s3.AmazonS3;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.S3SinkConnectorTestBase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
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
    props.put(S3SinkConnectorConfig.PART_SIZE_CONFIG, String.valueOf(100 * 1024 * 1024));
    props.put(S3SinkConnectorConfig.S3_BUFFER_TMP_DIR, "/mnt/tmp");
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
    int size = 20 * 1024 * 1024;  // 20 MB should trigger a 4-part multipart
    byte[] content = new byte[size];
    Arrays.fill(content, (byte) 'w');

    S3OutputStream stream = new S3OutputStream(
        "s3-output-stream-big-file-multipart-test", connectorConfig, realS3);
    stream.write(content);
    stream.commit();
  }

  @Test
  @Ignore
  public void testBinaryFile() throws Exception {
    InputStream is = new FileInputStream(new File("/tmp/foo.gz"));

    S3OutputStream stream = new S3OutputStream(
        "foo.copy.gz", connectorConfig, realS3);

    int totalRead = 0;
    int byteRead;
    while ((byteRead = is.read()) != -1) {
      stream.write(byteRead);
      totalRead++;
    }

    is.close();
    stream.commit();
    System.out.println("Total bytes read: " + totalRead);
  }
}
