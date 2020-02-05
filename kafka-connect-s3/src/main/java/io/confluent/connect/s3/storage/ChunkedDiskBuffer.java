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


// adapted from https://stackoverflow.com/questions/4332264/wrapping-a-bytebuffer-with-an-inputstream
@SuppressWarnings({"RedundantThrows", "NullableProblems"})
public class ChunkedDiskBuffer extends InputStream {
  private static final Logger log = LoggerFactory.getLogger(ChunkedDiskBuffer.class);

  private String fileNameRoot;
  List<Chunk> chunks = new ArrayList<>();
  private int chunkSize;
  private int currentChunkID = 0;
  private int currentReadingChunk = 0;
  private int numBytesRead = 0;

  ChunkedDiskBuffer(String bucket, String key, int chunkSize) {
    this.chunkSize = chunkSize;
    // TODO configurable location for tmp
    fileNameRoot = "/tmp/"
        + bucket.replaceAll("/", "-") + "-"
        + key.replaceAll("/", "-") + ".buffer";
    chunks.add(new Chunk(currentChunkID));
  }

  @Override
  public void close() throws IOException {
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
  }

  public void write(byte[] b, int off, int len) throws IOException {
    log.info("Writing an array of bytes");
    if (b == null) {
      throw new NullPointerException();
    } else if (outOfRange(off, b.length) || len < 0 || outOfRange(off + len, b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    if (chunks.get(currentChunkID).buffer.remaining() <= len) {
      int firstPart = chunks.get(currentChunkID).buffer.remaining();
      chunks.get(currentChunkID).buffer.put(b, off, firstPart);
      currentChunkID++;
      chunks.add(new Chunk(currentChunkID));
      write(b, off + firstPart, len - firstPart);
    } else {
      chunks.get(currentChunkID).buffer.put(b, off, len);
    }
  }

  private static boolean outOfRange(int off, int len) {
    return off < 0 || off > len;
  }

  public void rewind() {
    current().rewind();
  }

  private MappedByteBuffer current() {
    return chunks.get(currentReadingChunk).buffer;
  }

  public int read() throws IOException {
    try {
      if (!current().hasRemaining()) {
        // try the next chunk if this one has no more
        currentReadingChunk++;
        current().rewind();
        if (!current().hasRemaining()) {
          return -1;
        }
      }
    } catch (IndexOutOfBoundsException e) {
      return -1;
    }
    return current().get() & 0xFF;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public int read(byte[] bytes, int off, int len)
      throws IOException {
    try {
      if (!current().hasRemaining()) {
        // try the next chunk if this one has no more
        currentReadingChunk++;
        current().rewind();
        if (!current().hasRemaining()) {
          return -1;
        }
      }
    } catch (IndexOutOfBoundsException e) {
      // don't go past the end!
      return -1;
    }

    // when looping through our chunks:
    // bytes = our input array (never changed)
    // len = remaining bytes in current chunk, up to chunk size
    // off = chunk size * current chunk id
    int lengthThisChunk = Math.min(len, current().remaining());
    off = currentReadingChunk * chunkSize;
    current().get(bytes, off, lengthThisChunk);
    numBytesRead += lengthThisChunk;

    if (numBytesRead > 0 && numBytesRead < bytes.length) {
      read(bytes, currentReadingChunk * chunkSize, len);
    }

    return numBytesRead;
  }


  public class Chunk {
    private int id;
    MappedByteBuffer buffer;
    private File bufferFile;
    private RandomAccessFile bufferRandomAccessFile;
    private FileChannel bufferFileChannel;

    Chunk(int id) {
      // https://www.tothenew.com/blog/handling-large-files-using-javanio-mappedbytebuffer/
      // https://howtodoinjava.com/java7/nio/memory-mapped-files-mappedbytebuffer/
      this.id = id;
      log.info("Trying to init new MappedByteBuffer Chunk on {}", filename());
      try {
        bufferFile = new java.io.File(filename());
        if (bufferFile.exists()) {
          boolean deleted = bufferFile.delete();
          log.info("File {} already exists, deleted? {}", filename(), deleted);
        }
        if (bufferFile.createNewFile()) {
          bufferRandomAccessFile = new RandomAccessFile(bufferFile, "rw");
          bufferFileChannel = bufferRandomAccessFile.getChannel();
          this.buffer = bufferFileChannel.map(
              FileChannel.MapMode.READ_WRITE, 0, chunkSize);
        } else {
          throw new RuntimeException("File could not be created: " + filename());
        }
      } catch (FileNotFoundException fnfe) {
        log.error("FileNotFoundException opening file: " + filename());
        throw new RuntimeException(fnfe);
      } catch (IOException ioe) {
        log.error("IOException opening file: " + filename());
        throw new RuntimeException(ioe);
      }
      log.info("MappedByteBuffer Chunk was successfully initialized");
    }

    String filename() {
      return ChunkedDiskBuffer.this.fileNameRoot + "." + id;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    void close() throws IOException {
      try {
        buffer.clear();
        // make sure JVM deallocates mapped buffer so we can delete file
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

}
