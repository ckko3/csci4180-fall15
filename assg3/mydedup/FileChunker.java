package mydedup;

import java.util.*;
import java.util.ArrayList;
import java.io.File;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import static javax.xml.bind.DatatypeConverter.printHexBinary;

/**
 * split a InputStream as chunks
 * Give them to the given handler to upload
 */
public class FileChunker
{
    private static final String hash_func = "SHA-1";
    private static final int checksum_hexlen = 40;

    private MetadataStore.Mode mode;
    private Handler handler;
    private Charset ascii;

    public FileChunker(MetadataStore.Mode mode, Handler handler) {
        this.mode = mode;
        this.handler = handler;
        this.ascii = Charset.forName("US-ASCII");
    }

    public static class StoreException extends Exception {
        public StoreException(String message) {
             super(message);
        }
    }

    public void upload(InputStream in_stream, String uploaded_name,
            RabinChunker.Params params) throws StoreException, IOException {
        MetadataStore meta_store = new MetadataStore(mode);
        //meta_store.initReport();
        int s1 = 0;
        int s2 = 0;
        int total = 0;
        int unique = 0;
        //double space_saving;

        try {
            if (meta_store.fileExist(uploaded_name)) {
                throw new StoreException("File already exist in the store");
            }

            RabinChunker chunker = new RabinChunker(in_stream, params);
            /*
             * TODO: make it general to handle remote store
             */

            MessageDigest sha1sum = null;
            try {
                sha1sum = MessageDigest.getInstance(hash_func);
            } catch(java.security.NoSuchAlgorithmException e) {
                System.err.printf("This java runtime does not support %s\n", hash_func);
                System.exit(1);
            }

            ByteArrayOutputStream all_checksums = new ByteArrayOutputStream();
            RabinChunker.ChunkInfo chunki;

            while ((chunki = chunker.nextChunk()) != null) {
                sha1sum.update(chunki.chunk, 0, chunki.size);
                byte[] chunk_checksum = sha1sum.digest();
                String hex_checksum = printHexBinary(chunk_checksum);
                if (meta_store.isnew(hex_checksum)){
                    //System.out.printf("New chunk %s\n", new String(chunki.chunk));
                    /* make new metadata record */
                    meta_store.newChecksum(hex_checksum);
                    /* upload the chunk using its checksum as name */
                    handler.upload(hex_checksum, chunki.chunk, chunki.size);
                    //meta_store.uniqueChunkIncr();
                    unique++;
                    s2 += chunki.size;
                } else {
                    //System.out.printf("Depuplicated chunk %s\n", new String(chunki.chunk));
                    meta_store.refCountIncr(hex_checksum);
                    s1 += chunki.size;
                }

                all_checksums.write(hex_checksum.getBytes(this.ascii));
                //meta_store.totalChunkIncr();
                total++;
            }

            meta_store.newFileRecord(uploaded_name, all_checksums.toByteArray());
            meta_store.commit();

            /* report statistic */
            System.out.println("Report Output:");
            System.out.println("Total number of chunks in storage: " + total);
            System.out.println("Number of unique chunks in storage: " + unique);
            System.out.println("Number of bytes in storage with deduplication: " + s1);
            System.out.println("Number of bytes in storage without deduplication: " + s2);
            //space_saving = 1 - s1/s2;
            System.out.print("Space saving: ");
            System.out.printf("%.2f", 100*(1-(double)s1/(double)s2));
            System.out.println("%");

        } finally {
            meta_store.close();
        }
    }

    public void download(OutputStream dest, String uploaded_name)
            throws StoreException, IOException {
        MetadataStore meta_store = new MetadataStore(mode);
        int downloaded_byte = 0;
        int reconstructed_byte = 0;
        int downloaded_chunk = 0;

        try {
            if (!meta_store.fileExist(uploaded_name)) {
                throw new StoreException("File not exist in store");
            }

            byte[] checksum_all = meta_store.getFileRecord(uploaded_name);
            byte[] buf = new byte[2048];
            List chunk = new ArrayList();

            for (int i=0; i < checksum_all.length; i += checksum_hexlen) {
                boolean downloaded = false;
                String nth_checksum = new String(checksum_all, i, checksum_hexlen, this.ascii);
                InputStream chunk_stream = handler.download(nth_checksum);
                downloaded_chunk++;

                if(chunk.contains(nth_checksum)) {
                    downloaded = true;
                }
                else {
                    chunk.add(nth_checksum);
                }

                int count;
                while ((count = chunk_stream.read(buf)) >= 0) {
                    dest.write(buf, 0, count);
                    reconstructed_byte += count;
                    if(!downloaded) {
                        downloaded_byte += count;
                    }
                }

                /* if remote, delete chunks */
                if (mode == MetadataStore.Mode.REMOTE) {
                    File fpath = new File(nth_checksum);
                    fpath.delete();
                }
            }

            /* report statistic */
            System.out.println("Report Output:");
            System.out.println("Number of chunks downloaded: " + downloaded_chunk);
            System.out.println("Number of bytes downloaded: " + downloaded_byte);
            System.out.println("Number of bytes reconstructed: " + reconstructed_byte);

        } finally {
            /* In priciple, there is nothing to commit, but mapdb is opened in readwrite
             * mode, so have to do this anyway */
            meta_store.commit();
            meta_store.close();
        }
    }

    /* This method has many duplication with download.  I don't know how to refactor it
     * I miss ruby block */
    public void delete(String uploaded_name)
            throws StoreException {
        MetadataStore meta_store = new MetadataStore(mode);
        int deleted_chunk = 0;
        int deleted_byte = 0;

        try {
            if (!meta_store.fileExist(uploaded_name)) {
                throw new StoreException("File not exist in store");
            }

            byte[] checksum_all = meta_store.getFileRecord(uploaded_name);
            byte[] buf = new byte[2048];

            for (int i=0; i < checksum_all.length; i += checksum_hexlen) {
                String nth_checksum = new String(checksum_all, i, checksum_hexlen, this.ascii);
                if (meta_store.refCountDecr(nth_checksum) == 0) {
                    meta_store.deleteChecksum(nth_checksum);
                    deleted_byte += handler.delete(nth_checksum);
                    deleted_chunk++;
                }
            }
            meta_store.deleteFileRecord(uploaded_name);

            /* report statistic */
            System.out.println("Report Output:");
            System.out.println("Number of chunks deleted: " + deleted_chunk);
            System.out.println("Number of bytes deleted: " + deleted_byte);

        } finally {
            meta_store.commit();
            meta_store.close();
        }
    }
}
