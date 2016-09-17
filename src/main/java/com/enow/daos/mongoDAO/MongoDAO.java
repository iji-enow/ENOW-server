package com.enow.daos.mongoDAO;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.net.UnknownHostException;
import java.util.Set;

import org.bson.Document;

public class MongoDAO implements IMongoDAO{

	private MongoClient mongoClient;
	private MongoCollection<Document> collection;
	private MongoDatabase dbWrite;
	
	public MongoDAO(String url,int port) throws UnknownHostException {
		mongoClient = new MongoClient(url,port);
		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
	}
	
	public void setDBCollection(String dataBaseName,String collectionName){
		dbWrite = mongoClient.getDatabase(dataBaseName);
		collection = dbWrite.getCollection(collectionName);
	}
	
	public void setCollection(String collectionName){
		collection = dbWrite.getCollection(collectionName);
	}
	
	public MongoCollection<Document> getCollection(){
		return collection;
	}
	
	public int collectionCount(Document document){
		return (int)collection.count(document);
	}
	
	public FindIterable<Document> find(Document document){
		return collection.find(document);
	}
}
