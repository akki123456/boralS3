package awsS3;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class Main {
	
	public static void main(String args[]) throws IOException{
		AmazonS3 s3 = new AmazonS3Client(new ProfileCredentialsProvider());
		String bucketName = args[0];
		String keyForTheFile = args[1];
		String fileToBeUploaded = args[2];
		Bucket b = getBucket(s3, bucketName);
		if(b==null){
			b= createBucket(s3, bucketName);
		}
		
		if(uploadObject(s3, bucketName, keyForTheFile, fileToBeUploaded)){
			System.out.println("File Uploaded successfully");
			getAndReadFile(s3, bucketName, keyForTheFile);
		}
	}
	
	public static void getAndReadFile(AmazonS3 s3, String bucketName,
			String key ){
		try{
			S3Object object = s3.getObject(
	                new GetObjectRequest(bucketName, key));
			InputStream objectData = object.getObjectContent();
			InputStreamReader isr = new InputStreamReader(objectData);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			while((line=br.readLine())!=null){
				System.out.println(line);
			}
			objectData.close();
		}catch(AmazonServiceException ase){
			ase.printStackTrace();
		}catch(AmazonClientException ace){
			ace.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static boolean uploadObject(AmazonS3 s3, String bucketName,
			String key ,String  filepathTobeUploaded){
		try{
		File file = new File(filepathTobeUploaded);
		s3.putObject(bucketName, key, file);
		return true;
		}catch(AmazonServiceException ase){
			return false;
		}catch(AmazonClientException ace){
			return false;
		}
		
	}
	
	public static Bucket createBucket(AmazonS3 s3Client, String bucket_name){
		Bucket b = null;
		if (s3Client.doesBucketExist(bucket_name)) {
		    b = getBucket(s3Client,bucket_name);
		    System.out.println("bucket is there");
		} else {
		    try {
		        b = s3Client.createBucket(bucket_name);
		        System.out.println("created new bucket");
		    } catch (AmazonS3Exception e) {
		        System.err.println(e.getErrorMessage());
		    }
		}
		return b;
	}

	public static Bucket getBucket(AmazonS3 s3Client,String bucket_name) {
        Bucket resultBucket = null;
        List<Bucket> buckets = s3Client.listBuckets();
        for (Bucket b : buckets) {
            if (b.getName().equals(bucket_name)) {
            	resultBucket = b;
            }
        }
        return resultBucket;
}
	
}
