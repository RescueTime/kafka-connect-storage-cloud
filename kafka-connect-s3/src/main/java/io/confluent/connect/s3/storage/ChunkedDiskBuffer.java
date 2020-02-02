package io.confluent.connect.s3.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class ChunkedDiskBuffer {
  private static final Logger log = LoggerFactory.getLogger(ChunkedDiskBuffer.class);

  String filename;
  List<Chunk> chunks = new ArrayList<>();
  private int chunkSize;
  private int currentChunkID = 0;
  private Long position;

  public ChunkedDiskBuffer(String bucket, String key, int chunkSize) {
    this.chunkSize = chunkSize;
    filename = "/tmp/"
        + bucket.replaceAll("/", "-") + "-"
        + key.replaceAll("/", "-") + ".buffer";
    position = 0L;
    chunks.add(new Chunk(currentChunkID));
  }

  void closeBuffer() throws IOException {
    for (Chunk chunk : chunks) {
      chunk.close();
    }
  }

  public void write(int b) throws IOException {
    chunks.get(currentChunkID).buffer.put((byte) b);
    if (!chunks.get(currentChunkID).buffer.hasRemaining()) {
      // start a new chunk
      currentChunkID++;
      chunks.add(new Chunk(currentChunkID));
    }
    position++;
  }

  ByteBufferBackedInputStream getInputStream() {
    return new ByteBufferBackedInputStream(chunks.get(currentChunkID).buffer);
  }

  public class Chunk {
    private int id;
    MappedByteBuffer buffer;
    private File bufferFile;
    private RandomAccessFile bufferRandomAccessFile;
    private FileChannel bufferFileChannel;

    public Chunk(int id) {
      // https://www.tothenew.com/blog/handling-large-files-using-javanio-mappedbytebuffer/
      // https://howtodoinjava.com/java7/nio/memory-mapped-files-mappedbytebuffer/
      this.id = id;
      log.info("Trying to init new MappedByteBuffer Chunk on {}", getFilename());
      try {
        bufferFile = new java.io.File(getFilename());
        if (bufferFile.exists()) {
          boolean deleted = bufferFile.delete();
          log.info("File {} already exists, deleted? {}", getFilename(), deleted);
        }
        if (bufferFile.createNewFile()) {
          bufferRandomAccessFile = new RandomAccessFile(bufferFile, "rw");
          bufferFileChannel = bufferRandomAccessFile.getChannel();
          this.buffer = bufferFileChannel.map(
              FileChannel.MapMode.READ_WRITE, 0, chunkSize);
        } else {
          throw new RuntimeException("File could not be created: " + getFilename());
        }
      } catch (FileNotFoundException fnfe) {
        log.error("FileNotFoundException opening file: " + getFilename());
        throw new RuntimeException(fnfe);
      } catch (IOException ioe) {
        log.error("IOException opening file: " + getFilename());
        throw new RuntimeException(ioe);
      }
      log.info("MappedByteBuffer Chunk was successfully initialized");
    }

    String getFilename() {
      return ChunkedDiskBuffer.this.filename + "." + id;
    }

    void close() throws IOException {
      try {
        buffer.clear();
        Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
        if (cleaner != null) {
          cleaner.clean();
        }
      } finally {
        bufferFileChannel.close();
        bufferRandomAccessFile.close();
        bufferFile.delete();
        bufferFile = null;
        bufferRandomAccessFile = null;
        bufferFileChannel = null;
        buffer = null;
      }
    }
  }

  // copied from https://stackoverflow.com/questions/4332264/wrapping-a-bytebuffer-with-an-inputstream
  public class ByteBufferBackedInputStream extends InputStream {

    MappedByteBuffer buf;

    public ByteBufferBackedInputStream(MappedByteBuffer buf) {
      this.buf = buf;
      this.buf.rewind();
    }

    public int read() throws IOException {
      if (!buf.hasRemaining()) {
        return -1;
      }
      return buf.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len)
        throws IOException {
      if (!buf.hasRemaining()) {
        return -1;
      }

      len = Math.min(len, buf.remaining());
      buf.get(bytes, off, len);
      return len;
    }
  }

}
