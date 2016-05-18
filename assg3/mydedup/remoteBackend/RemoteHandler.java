package mydedup.remoteBackend;

import mydedup.Handler;

import java.io.IOException;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.File;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.net.URISyntaxException;
import java.util.*;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.blob.client.*;

public class RemoteHandler implements Handler
{
    public static final String storageConnectionString =
                    "DefaultEndpointsProtocol=http;" +
                    "AccountName=csci418010;" +
                    "AccountKey=KIdI54ayKJK61GIxCjQ4slFpSuiD+4PGkUqyv9pZCv/edxhVqaOpFWqEtoY11DSAd/24AnsGnvJ16UcT1HERrg==";

    public void upload(String name, byte[] content, int size) {
        File fpath = new File(name);
        try (FileOutputStream outStream = new FileOutputStream(fpath)) {
            outStream.write(content, 0, size);
        } catch (IOException e) {
            System.err.println("Error writing chunk to local: " + e.getMessage());
            System.exit(1);
        }

        try {
            //set JVM proxy to use CSE proxy
            System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
            System.setProperty("http.proxyPort", "8000");

            //retrieve cloud storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

            //Create blob client
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

            // Get a reference to a container
            // The container name must be lower case
            CloudBlobContainer container = blobClient.getContainerReference("mycontainer");

            // Create the container if it does not exist
            container.createIfNotExist();

            String fname = name;
            CloudBlockBlob blob = container.getBlockBlobReference(fname);
            File img = new File(fname);
            blob.upload(new FileInputStream(img),img.length());

        } catch (InvalidKeyException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (StorageException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        fpath.delete();
    }
    
    public FileInputStream download(String name) {

        try {
            System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
            System.setProperty("http.proxyPort", "8000");

            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString    );
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            CloudBlobContainer container = blobClient.getContainerReference("mycontainer");
            container.createIfNotExist();

            String fname = name;
            CloudBlockBlob blob = container.getBlockBlobReference(fname);
            blob.download(new FileOutputStream(fname,true));

        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File fpath = new File(name);
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(fpath);
        } catch (FileNotFoundException e) {
            System.err.printf("Error: chunk %s is not found\n", name);
            System.exit(1);
        }
            return stream;
   }

    public long delete(String name) {
        /* download and read file size */
        try {
            System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
            System.setProperty("http.proxyPort", "8000");

            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString    );
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            CloudBlobContainer container = blobClient.getContainerReference("mycontainer");
            container.createIfNotExist();

            String fname = name;
            CloudBlockBlob blob = container.getBlockBlobReference(fname);
            blob.download(new FileOutputStream(fname,true));

        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File fpath = new File(name);
        long deleted_byte = fpath.length();
        fpath.delete();

        /* real delete */
        try {
            System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
            System.setProperty("http.proxyPort", "8000");
            
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            CloudBlobContainer container = blobClient.getContainerReference("mycontainer");
            container.createIfNotExist();

            String fname = name;
            CloudBlockBlob blob = container.getBlockBlobReference(fname);
            blob.delete();

        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        }

        return deleted_byte;
    }
}
