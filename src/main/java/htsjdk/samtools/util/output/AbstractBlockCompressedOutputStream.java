package htsjdk.samtools.util.output;

import htsjdk.samtools.util.*;
import htsjdk.samtools.util.zip.DeflaterFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.Deflater;

public abstract class AbstractBlockCompressedOutputStream extends OutputStream implements LocationAware {

    protected static int defaultCompressionLevel = BlockCompressedStreamConstants.DEFAULT_COMPRESSION_LEVEL;
    protected static DeflaterFactory defaultDeflaterFactory = new DeflaterFactory();

    protected final BinaryCodec codec;
    protected final byte[] uncompressedBuffer = new byte[BlockCompressedStreamConstants.DEFAULT_UNCOMPRESSED_BLOCK_SIZE];
    protected int numUncompressedBytes = 0;

    protected Path file;
    protected long mBlockAddress = 0;
    protected GZIIndex.GZIIndexer indexer;

    // Really a local variable, but allocate once to reduce GC burden.
    protected final byte[] singleByteArray = new byte[1];

    /**
     * Uses default compression level, which is 5 unless changed by setCompressionLevel
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     */
    public AbstractBlockCompressedOutputStream(final Path file) {
        this.file = file;
        codec = new BinaryCodec(file, true);
    }

    public AbstractBlockCompressedOutputStream(final OutputStream os, final Path file) {
        this.file = file;
        codec = new BinaryCodec(os);
        if (file != null) {
            codec.setOutputFileName(file.toAbsolutePath().toUri().toString());
        }
    }

    /**
     * Writes b.length bytes from the specified byte array to this output stream. The general contract for write(b)
     * is that it should have exactly the same effect as the call write(b, 0, b.length).
     * @param bytes the data
     */
    @Override
    public void write(final byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    /**
     * Writes len bytes from the specified byte array starting at offset off to this output stream. The general
     * contract for write(b, off, len) is that some of the bytes in the array b are written to the output stream in order;
     * element b[off] is the first byte written and b[off+len-1] is the last byte written by this operation.
     *
     * @param bytes the data
     * @param startIndex the start offset in the data
     * @param numBytes the number of bytes to write
     */
    @Override
    public void write(final byte[] bytes, int startIndex, int numBytes) throws IOException {
        assert(numUncompressedBytes < uncompressedBuffer.length);
        while (numBytes > 0) {
            final int bytesToWrite = Math.min(uncompressedBuffer.length - numUncompressedBytes, numBytes);
            System.arraycopy(bytes, startIndex, uncompressedBuffer, numUncompressedBytes, bytesToWrite);
            numUncompressedBytes += bytesToWrite;
            startIndex += bytesToWrite;
            numBytes -= bytesToWrite;
            assert(numBytes >= 0);
            if (numUncompressedBytes == uncompressedBuffer.length) {
                deflateBlock();
            }
        }
    }

    /**
     * WARNING: flush() affects the output format, because it causes the current contents of uncompressedBuffer
     * to be compressed and written, even if it isn't full.  Unless you know what you're doing, don't call flush().
     * Instead, call close(), which will flush any unwritten data before closing the underlying stream.
     *
     */
    @Override
    public void flush() throws IOException {
        while (numUncompressedBytes > 0) {
            deflateBlock();
        }
        codec.getOutputStream().flush();
    }

    public void close(final boolean writeTerminatorBlock) throws IOException {
        flush();
        // For debugging...
        // if (numberOfThrottleBacks > 0) {
        //     System.err.println("In BlockCompressedOutputStream, had to throttle back " + numberOfThrottleBacks +
        //                        " times for file " + codec.getOutputFileName());
        // }
        if (writeTerminatorBlock) {
            codec.writeBytes(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
        }
        codec.close();
        if (indexer != null) {
            indexer.close();
        }
        // Can't re-open something that is not a regular file, e.g. a named pipe or an output stream
        if (this.file == null || !Files.isRegularFile(this.file)) return;
        if (BlockCompressedInputStream.checkTermination(this.file) !=
                BlockCompressedInputStream.FileTermination.HAS_TERMINATOR_BLOCK) {
            throw new IOException("Terminator block not found after closing BGZF file " + this.file);
        }
    }

    /**
     * Writes the specified byte to this output stream. The general contract for write is that one byte is written
     * to the output stream. The byte to be written is the eight low-order bits of the argument b.
     * The 24 high-order bits of b are ignored.
     * @param bite
     * @throws IOException
     */
    @Override
    public void write(final int bite) throws IOException {
        singleByteArray[0] = (byte)bite;
        write(singleByteArray);
    }

    /** Encode virtual file pointer
     * Upper 48 bits is the byte offset into the compressed stream of a block.
     * Lower 16 bits is the byte offset into the uncompressed stream inside the block.
     */
    public long getFilePointer(){
        return BlockCompressedFilePointerUtil.makeFilePointer(mBlockAddress, numUncompressedBytes);
    }

    @Override
    public long getPosition() {
        return getFilePointer();
    }

    /**
     * Attempt to write the data in uncompressedBuffer to the underlying file in a gzip block.
     * If the entire uncompressedBuffer does not fit in the maximum allowed size, reduce the amount
     * of data to be compressed, and slide the excess down in uncompressedBuffer so it can be picked
     * up in the next deflate event.
     */
    protected abstract void deflateBlock();

    /**
     * Writes the entire gzip block, assuming the compressed data is stored in compressedBuffer
     * @return  size of gzip block that was written.
     */
    protected int writeGzipBlock(final byte[] compressedBuffer, final int compressedSize, final int uncompressedSize, final long crc) {
        // Init gzip header
        codec.writeByte(BlockCompressedStreamConstants.GZIP_ID1);
        codec.writeByte(BlockCompressedStreamConstants.GZIP_ID2);
        codec.writeByte(BlockCompressedStreamConstants.GZIP_CM_DEFLATE);
        codec.writeByte(BlockCompressedStreamConstants.GZIP_FLG);
        codec.writeInt(0); // Modification time
        codec.writeByte(BlockCompressedStreamConstants.GZIP_XFL);
        codec.writeByte(BlockCompressedStreamConstants.GZIP_OS_UNKNOWN);
        codec.writeShort(BlockCompressedStreamConstants.GZIP_XLEN);
        codec.writeByte(BlockCompressedStreamConstants.BGZF_ID1);
        codec.writeByte(BlockCompressedStreamConstants.BGZF_ID2);
        codec.writeShort(BlockCompressedStreamConstants.BGZF_LEN);
        final int totalBlockSize = compressedSize + BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH +
                BlockCompressedStreamConstants.BLOCK_FOOTER_LENGTH;

        // I don't know why we store block size - 1, but that is what the spec says
        codec.writeShort((short)(totalBlockSize - 1));
        codec.writeBytes(compressedBuffer, 0, compressedSize);
        codec.writeInt((int)crc);
        codec.writeInt(uncompressedSize);
        return totalBlockSize;
    }

    /**
     * Sets the default {@link DeflaterFactory} that will be used for all instances unless specified otherwise in the constructor.
     * If this method is not called the default is a factory that will create the JDK {@link Deflater}.
     * @param deflaterFactory non-null default factory.
     */
    public static void setDefaultDeflaterFactory(final DeflaterFactory deflaterFactory) {
        if (deflaterFactory == null) {
            throw new IllegalArgumentException("null deflaterFactory");
        }
        defaultDeflaterFactory = deflaterFactory;
    }

    public static DeflaterFactory getDefaultDeflaterFactory() {
        return defaultDeflaterFactory;
    }

    /**
     * Sets the GZip compression level for subsequent BlockCompressedOutputStream object creation
     * that do not specify the compression level.
     * @param compressionLevel 1 <= compressionLevel <= 9
     */
    public static void setDefaultCompressionLevel(final int compressionLevel) {
        if (compressionLevel < Deflater.NO_COMPRESSION || compressionLevel > Deflater.BEST_COMPRESSION) {
            throw new IllegalArgumentException("Invalid compression level: " + compressionLevel);
        }
        defaultCompressionLevel = compressionLevel;
    }

    public static int getDefaultCompressionLevel() {
        return defaultCompressionLevel;
    }
}
