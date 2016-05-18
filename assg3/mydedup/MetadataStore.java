package mydedup;

import java.io.IOException;
import java.io.File;
import java.util.AbstractMap;
import java.util.Set;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class MetadataStore
{
    public static final String db_name = "mydedup.meta";
    private DB db;
    private AbstractMap<String, byte[]> checksum_store;
    private AbstractMap<String, Integer> refcount_store;
    //private AbstractMap<String, Integer> chunk_store;
   
    public enum Mode { LOCAL, REMOTE }

    public MetadataStore(Mode mode) {
        db = DBMaker
                 .fileDB(new File(db_name))
                 .fileMmapEnableIfSupported() // enable mmap on supported platforms
                 .fileMmapCleanerHackEnable() // closes file on DB.close()
                 .make();

        checksum_store = db.treeMapCreate("checksum-" + mode.toString())
                 .keySerializer(Serializer.STRING)
                 .valueSerializer(Serializer.BYTE_ARRAY)
                 .makeOrGet();
        refcount_store = db.treeMapCreate("refcount-" + mode.toString())
                 .keySerializer(Serializer.STRING_ASCII)
                 .valueSerializer(Serializer.INTEGER)
                 .makeOrGet();
        /*chunk_store = db.treeMapCreate("chunk-" + mode.toString())
                 .keySerializer(Serializer.STRING)
                 .valueSerializer(Serializer.INTEGER)
                 .makeOrGet();*/

    }

    /*
     * Report if a chunk represented by its checksum is new (does not exist)
     */
    public boolean isnew(String checksum) {
        return !(refcount_store.containsKey(checksum));
    }

    public boolean fileExist(String name) {
        return checksum_store.containsKey(name);
    }

    public void newChecksum(String checksum) {
        refcount_store.put(checksum, 1);
    }

    public void deleteChecksum(String checksum) {
        refcount_store.remove(checksum);
    }

    public void refCountIncr(String checksum) {
        int now_count = refcount_store.get(checksum);
        refcount_store.put(checksum, now_count + 1);
    }

    public int refCountDecr(String checksum) {
        int now_count = refcount_store.get(checksum);
        now_count--;
        refcount_store.put(checksum, now_count);
        return now_count;
    }

    public void newFileRecord(String name, byte[] checksums) {
        checksum_store.put(name, checksums);
    }

    public void deleteFileRecord(String name) {
        checksum_store.remove(name);
    }

    public byte[] getFileRecord(String name) {
        return checksum_store.get(name);
    }

    public Set<String> listFile() {
        return checksum_store.keySet();
    }
    /*
    public void initReport() {
        if(!chunk_store.containsKey("total")) {
            chunk_store.put("total", 0);
        }
        if(!chunk_store.containsKey("unique")) {
            chunk_store.put("unique", 0);
        }
    }

    public int getTotalChunk() {
        return chunk_store.get("total");
    }

    public int getUniqueChunk() {
        return chunk_store.get("unique");
    }

    public void totalChunkIncr() {
        int now_count = chunk_store.get("total");
        now_count++;
        chunk_store.put("total", now_count);
    }

    public void uniqueChunkIncr() {
        int now_count = chunk_store.get("unique");
        now_count++;
        chunk_store.put("unique", now_count);
    }
    */
    public void commit() {
        db.commit();
    }

    public void close() {
        db.close();
    }
}
