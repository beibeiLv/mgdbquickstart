package com.nancy.mgdbQuicklyStart;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class Quicktour {
	public static void main(String args[]) {
		MongoClient client = new MongoClient(new MongoClientURI("mongodb://10.64.222.205:27017"));
		MongoDatabase database = client.getDatabase("test");

		MongoCollection<Document> collection = database.getCollection("test");
		collection.drop();
		
		// Insert 3 docs 
		Document doc1 = new Document("name", "OracleDB")
				.append("type", "database").append("count", 1)
				.append("version", Arrays.asList("V3.2", "V3.0", "v2.6"))
				.append("info", new Document("x", 203).append("y", 102));


		Document doc2 = new Document("name", "MongoDB")
				.append("type", "database").append("count", 5)
				.append("version", Arrays.asList("V3.2", "V3.1", "v3.2"))
				.append("info", new Document("x", 201).append("y", 108));
		

		Document doc3 = new Document("name", "MySqlDb")
				.append("type", "database").append("count", 9)
				.append("version", Arrays.asList("V3.2", "V3.1", "v3.3"))
				.append("info", new Document("x", 205).append("y", 105)).append("CreateBy", "belv");
		
		
		List <Document> doclist = new ArrayList<Document>();
		doclist.add(doc1);
		doclist.add(doc2);
		doclist.add(doc3);
		//1. test insert
		testInsert(collection,doclist);
		
		//2. test find
		// find according to the criteria
		//		The first kind of iterator		
		FindIterable<Document> findIterable = collection.find(new BsonDocument(
				"version", new BsonArray(Arrays.asList(new BsonString("V3.2"),
						new BsonString("V3.1"), new BsonString("v3.2")))));
		MongoCursor<Document> mongoCursor = findIterable.iterator();
		System.out.println("The select record:");
		while (mongoCursor.hasNext()){
			System.out.println(mongoCursor.next().toJson());
		}
		

		//3. test update
		System.out.println("begin to update: ");
		testUpdate(collection, Filters.eq("count", 1), new BsonDocument("$set", new BsonDocument("count",new BsonInt32(5))));
		
		FindIterable<Document> findIterable2 = testFind(collection, Filters.eq("type", "database"));
		
		//		The second iterator
		System.out.println("Print all :");
		for(Document doc : findIterable2){
			
			System.out.println(doc.toJson());
		}
		// test delete
		System.out.println("begin to delete that count eq 5: ");
		testDeleteMany(collection, Filters.eq("count", 5));
		
		System.out.println("Print all that after deleted :");
		FindIterable<Document> findIterable3 = testFind(collection,  Filters.eq("type", "database"));
		
//		The second iterator
		
		for(Document doc : findIterable3){
			
			System.out.println(doc.toJson());
		}
		client.close();
	}

	public static void testInsert(MongoCollection<Document> collection,List<? extends Document> docList ) {
		collection.insertMany(docList);
	}
	public static FindIterable<Document> testFind(MongoCollection<Document> collection,Bson filter) {
		return collection.find(filter);
	}
	public static void testUpdate(MongoCollection<Document> collection,Bson filter, Bson update) {
		collection.updateMany(filter, update);
	}

	public static void testDeleteOne(MongoCollection<Document> collection,Bson filter) {
		collection.deleteOne(filter);
	}
	public static void testDeleteMany(MongoCollection<Document> collection,Bson filter) {
		collection.deleteMany(filter);
	}
}
