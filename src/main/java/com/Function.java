package com;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.bulkexecutor.BulkImportResponse;
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor;
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor.Builder;
import com.microsoft.azure.functions.*;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
 
/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {

    private static final String CONNECT_STRING = 
        "BlobStorage_Connection_String";
    
    private static final String CONTAINER_NAME = "testcontainer";
    
    private static final String COSMOSDB_ACCOUNT_NAME = "abinav-bulkexecutor-splitproof";
    private static final String COSMOSDB_ACCOUNT_KEY = "CosmosDB_Account_Key";
    private static final String DATABASE_NAME = "SplitProofDB";
    private static final String COLLECTION_NAME = "tempCollection3";
    
    private DocumentClient documentClient;    
    private DocumentBulkExecutor bulkExecutor;
    private ExecutionContext context;
    
    @FunctionName("BlobProcessor")
    public void run(
        @BlobTrigger(name = "file", path = "testcontainer/{name}", dataType = "binary", connection = "StorageAccountName") byte[] content,
        @BindingName("name") String name,
        final ExecutionContext context) {
 
        this.context = context;
        context.getLogger().info("Java Blob trigger function processed a blob. Name: " + name + " Bytes" + content.length);
        
        CloudStorageAccount storageAccount;
        CloudBlobClient blobClient = null;
        CloudBlobContainer container = null;
        
        try {
            storageAccount = CloudStorageAccount.parse(CONNECT_STRING);
            blobClient = storageAccount.createCloudBlobClient();
            container = blobClient.getContainerReference(CONTAINER_NAME);
            
            // Get the CloudBlob references to the recently added file in the container
            CloudBlob blob = container.getBlobReferenceFromServer(name);
            
            // Initialize the DocumentClient
            this.bootup(COSMOSDB_ACCOUNT_NAME, COSMOSDB_ACCOUNT_KEY);
            
            // Initialize the BulkExecutor library
            this.bootupBulkExecutor(DATABASE_NAME, COLLECTION_NAME);
            
            // Download the newly uploaded blob file
            this.downloadBlobFile(blob, context);
            
            // Upload the contents of the file to Azure Cosmos DB using the Cosmos DB Bulk Executor
            this.uploadBlobFileContentsToCosmosDB(blob.getName(), context);
            
        } catch (StorageException ex) {
            System.out.println(String.format("Error returned from the service. Http code: %d and error code: %s", ex.getHttpStatusCode(), ex.getErrorCode()));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
    
    private void downloadBlobFile(CloudBlob blob, ExecutionContext context) {
        try {
            blob.download(new FileOutputStream(blob.getName()));
            context.getLogger().info("Successfully download blob: " + blob.getName());
        } catch (StorageException e) {
            context.getLogger().info(
                "Storage exception thrown when downloading the contents of blob file: " + 
                blob.getName() + 
                " Stack trace: " + 
                e.getStackTrace());
            
        } catch (FileNotFoundException e) {
            context.getLogger().info(
                "FileNotFoundException thrown when downloading blob into local file. " + 
                " Stack trace: " + 
                e.getStackTrace());
        }
    }
    
    private void uploadBlobFileContentsToCosmosDB(String fileName, ExecutionContext context) {
        BufferedReader bufferedReader;
        List<String> documentsToIngestIntoCosmosDB = new ArrayList<>();
        try {
            bufferedReader = new BufferedReader(new FileReader(fileName));
            String line = bufferedReader.readLine();
            while (line != null) {
                line = bufferedReader.readLine();
                documentsToIngestIntoCosmosDB.add(line);
            }
            
            context.getLogger().info("Stored all JSON documents in memory");
            
            try {
                if(bulkExecutor == null) {
                    context.getLogger().info("BulkExecutor is null!");
                }
                
                context.getLogger().info("Size of input list = " + documentsToIngestIntoCosmosDB.size());
                
                BulkImportResponse bulkImportResponse = bulkExecutor.importAll(documentsToIngestIntoCosmosDB, false, false, null);
                context.getLogger().info("Completed bulk load!");
                
                context.getLogger().info(
                    "Number of documents inserted: " +
                    bulkImportResponse.getNumberOfDocumentsImported() + 
                    "Total RU consumption for batch: " + 
                    bulkImportResponse.getTotalRequestUnitsConsumed());
                
            } catch (DocumentClientException e) {
                context.getLogger().info(
                    "Exception encountered when inserting documents in bulk. " +
                    "Exception message was: " + 
                    e.getMessage() + 
                    "Status code = " + 
                    e.getStatusCode());  
                
                e.printStackTrace();
            }
            
        } catch (IOException ex) {
            context.getLogger().info(
                "Exception thrown when reading from local file. " + 
                " Stack trace: " + 
                ex.getStackTrace());
        }
    }
    
    private void bootup(String accountName, String accountKey)
    {
        String hostnamePrefix = "https://";     
        String hostnamePostfix = ".documents.azure.com:443/";
        
        try
        {
            String host = hostnamePrefix + accountName + hostnamePostfix;
            String key = accountKey;
             
            ConnectionPolicy connectionPolicy = new ConnectionPolicy();
            connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);
            connectionPolicy.setMaxPoolSize(5000);
            
            this.documentClient = new DocumentClient(host, key, connectionPolicy, ConsistencyLevel.Eventual);
            context.getLogger().info("Successfully instantiated the DocumentClient");
        }
        catch(Exception e)
        {
            context.getLogger().info(
                "Exception thrown when initializing the DocumentClient. " + 
                "Stack trace is: " + 
                e.getStackTrace());
        }
    }
    
    private void bootupBulkExecutor(String databaseName, String collectionName) {
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        Collection<String> paths = new ArrayList<String>();
        paths.add("/age");
        partitionKeyDefinition.setPaths(paths);
        Builder bulkExecutorBuilder = DocumentBulkExecutor.builder().from(this.documentClient, databaseName,
                collectionName, partitionKeyDefinition, 4000);
        
        try {
            this.bulkExecutor = bulkExecutorBuilder.build();
            context.getLogger().info("Successfully instantiated the BulkExecutor");
        } catch (Exception e) {
            
            context.getLogger().info(
                "Exception thrown when initializing the BulkExecutor. " + 
                "Stack trace is: " + 
                e.getStackTrace());
        }
    }
}
