package org.myorg;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.blob.client.*;

public class AzureAPIDemo {
	public static final String storageConnectionString = 
    	    "DefaultEndpointsProtocol=http;" + 
    	    "AccountName=csci418010;" + 
    	    "AccountKey=KIdI54ayKJK61GIxCjQ4slFpSuiD+4PGkUqyv9pZCv/edxhVqaOpFWqEtoY11DSAd/24AnsGnvJ16UcT1HERrg==";
	
	public static void main(String[] args){
		
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
			
			if ( args.length == 2 && args[0].equals("upload") ){
				String fname = args[1];
				CloudBlockBlob blob = container.getBlockBlobReference(fname);

				File img = new File(fname);
				blob.upload(new FileInputStream(img),img.length());
			} else if (args.length == 2 && args[0].equals("download")){
				String fname = args[1];
				CloudBlockBlob blob = container.getBlockBlobReference(fname);
				blob.download(new FileOutputStream("azure_"+fname,true));
			} else if (args.length == 2 && args[0].equals("delete")){
				String fname = args[1];
				CloudBlockBlob blob = container.getBlockBlobReference(fname);
				blob.delete();
			} else if (args.length == 1 && args[0].equals("show")){
				for (ListBlobItem blobItem:container.listBlobs()){
				    System.out.println(blobItem.getUri());
				}
			}else {
				System.out.println("Usage:");
				System.out.println("For file upload: upload <file_to_upload>");
				System.out.println("For file download: download <file_to_download>");
				System.out.println("For file deletion: delete <file_to_delete>");
				System.out.println("For file exploration: show");
			}
		}catch (InvalidKeyException e) {
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
	}

}
