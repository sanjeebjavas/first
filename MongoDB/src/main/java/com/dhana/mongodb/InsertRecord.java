package com.dhana.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
 
public class InsertRecord {
 
    private static Logger log = Logger.getLogger(InsertRecord.class);
 
    // Adding a single document into the mongo collection.
    private static void addOneDocument(MongoCollection<Document> col) {
 
        // Sample document.
        Document emp1 = new Document();
        emp1.put("name", "yatin batra");
        emp1.put("website", "javacodegeeks.com");
 
        Document emp1_addr = new Document();
        emp1_addr.put("addr_line1", "Savannah, Illinois");      
        emp1_addr.put("zip_code", "85794");
        emp1.put("address", emp1_addr);
 
        col.insertOne(emp1);
    }
 
    // Adding the multiple documents into the mongo collection.
    private static void addMultipleDocuments(MongoCollection<Document> col) {
 
        // Sample document.
        Document emp2 = new Document();
        emp2.put("name", "Charlotte Neil");
        emp2.put("website", "webcodegeeks.com");
 
        Document emp2_addr = new Document();
        emp2_addr.put("addr_line1", "Fremont, AK");
        emp2_addr.put("zip_code", "19408");
        emp2.put("address", emp2_addr);
 
        // Sample document.
        Document emp3 = new Document();
        emp3.put("title", "Ms.");
        emp3.put("name", "Samantha Greens");
        emp3.put("website", "systemcodegeeks.com");
 
        Document emp3_addr = new Document();
        emp3_addr.put("addr_line1", "Cudahy, Ohio");
        emp3_addr.put("zip_code", "31522");
        emp3.put("address", emp3_addr);
 
        // Adding documents to a list.
        List<Document> docs = new ArrayList<Document>();
        docs.add(emp2);
        docs.add(emp3);
 
        col.insertMany(docs);
    }
 
    // Fetching all documents from the mongo collection.
    private static void getAllDocuments(MongoCollection<Document> col) {
        log.info("Fetching all documents from the collection");
 
        // Performing a read operation on the collection.
       // col.updateOne(Filters.eq("oid", "5daeb3ba7bb1cb4d9d9feeb1"), Updates.set("name", "dhana"));
        //col.deleteOne(Filters.eq("oid","5daeb06a515b7f07d254a434")); 
		FindIterable<Document> fi = col.find();
		MongoCursor<Document> cursor = fi.iterator();
		try {
			while (cursor.hasNext()) {
				System.out.println((((cursor.next().toJson()))));
			}
		} finally {
			cursor.close();
		}
		 
    
		col.updateOne(Filters.eq("oid", "5daeb06a515b7f07d254a434"), Updates.set("name", "dhana"));
   
}
    @SuppressWarnings("resource")
    public static void main(String[] args) {
 
        // Mongodb initialization parameters.
        int port_no = 27017;
        String host_name = "localhost", db_name = "dhana", db_coll_name = "emp";
 
        // Mongodb connection string.
        String client_url = "mongodb://" + host_name + ":" + port_no + "/" + db_name;
        MongoClientURI uri = new MongoClientURI(client_url);
 
        // Connecting to the mongodb server using the given client uri.
        MongoClient mongo_client = new MongoClient(uri);
 
        // Fetching the database from the mongodb.
        MongoDatabase db = mongo_client.getDatabase(db_name);
 
        // Fetching the collection from the mongodb.
        MongoCollection<Document> coll = db.getCollection(db_coll_name);
        coll.updateOne(Filters.eq("oid", "5daeb06a515b7f07d254a434"), Updates.set("name", "dhana"));
 
        // Adding a single document in the mongo collection.
        //addOneDocument(coll);
 
        //log.info("\n");
 
        // Adding the multiple documents in the mongo collection.
       // addMultipleDocuments(coll);
 
        //log.info("\n");
 
        // Fetching all the documents from the mongodb.
        getAllDocuments(coll);
    }   
}
