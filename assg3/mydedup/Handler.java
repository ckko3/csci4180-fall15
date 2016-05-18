package mydedup;

import java.io.InputStream;

public interface Handler {
    public void upload(String name, byte[] content, int size);
    public InputStream download(String name);
    public long delete(String name);
}
