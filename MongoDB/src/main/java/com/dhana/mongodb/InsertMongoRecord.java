package com.dhana.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.combine;
 
public class InsertMongoRecord {
 
    private static Logger log = Logger.getLogger(InsertMongoRecord.class);
 
    // Adding a single document into the mongo collection.
    private static void addOneDocument(MongoCollection<Document> col) {
 
        // Sample document.
        Document emp1 = new Document();
        emp1.put("id",1);
        emp1.put("name", "dhana");
        emp1.put("website", "javacodegeeks.com");
 
        Document emp1_addr = new Document();
        emp1_addr.put("addr_line1", "bhoipada");      
        emp1_addr.put("zip_code", "754105");
        emp1.put("address", emp1_addr);
 
        col.insertOne(emp1);
    }
 
    // Adding the multiple documents into the mongo collection.
    private static void addMultipleDocuments(MongoCollection<Document> col) {
 
        // Sample document.
        Document emp2 = new Document();
        emp2.put("id",2);
        emp2.put("name", "Ganeswar Nayak");
        emp2.put("website", "webcodegeeks.com");
 
        Document emp2_addr = new Document();
        emp2_addr.put("addr_line1", "Bangalore");
        emp2_addr.put("zip_code", "560037");
        emp2.put("address", emp2_addr);
 
        // Sample document.
        Document emp3 = new Document();
        emp3.put("id",3);
        emp3.put("title", "Mr.");
        emp3.put("name", "Sanjeeb Sahoo");
        emp3.put("website", "systemcodegeeks.com");
 
        Document emp3_addr = new Document();
        emp3_addr.put("addr_line1", "Eranch");
        emp3_addr.put("zip_code", "754105");
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
				System.out.println(((cursor.next().toJson())));
			}
		} finally {
			cursor.close();
		}
   
}
    private static void updateDocuments(MongoCollection<Document> col) {
    	Bson filter = null;
    	Bson query = null;
     
    	//col.updateOne(Filters.eq("oid","5daee832a224cd7c1c60dbea"), Updates.set("name", "Gani"));
    	filter = eq("name", "Ganeswar");
		query = combine(set("age", 23), set("gender", "Male"));
		UpdateResult result = col.updateMany(filter, query);
	} 
    @SuppressWarnings("resource")
    public static void main(String[] args) {
 
        // Mongodb initialization parameters.
        int port_no = 27017;
        String host_name = "localhost", db_name = "dhana", db_coll_name = "employee";
 
        // Mongodb connection string.
        String client_url = "mongodb://" + host_name + ":" + port_no + "/" + db_name;
        MongoClientURI uri = new MongoClientURI(client_url);
 
        // Connecting to the mongodb server using the given client uri.
        MongoClient mongo_client = new MongoClient(uri);
 
        // Fetching the database from the mongodb.
        MongoDatabase db = mongo_client.getDatabase(db_name);
 
        // Fetching the collection from the mongodb.
        MongoCollection<Document> coll = db.getCollection(db_coll_name);
 
        // Adding a single document in the mongo collection.
        //addOneDocument(coll);
 
        //log.info("\n");
 
        // Adding the multiple documents in the mongo collection.
        //addMultipleDocuments(coll);
 
        //log.info("\n");
 
        // Fetching all the documents from the mongodb.
        getAllDocuments(coll);
        
        //updateDocuments(coll);
        
        //getAllDocuments(coll);
    }

	  
}
