package com.enow.daos.mongoDAO;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

public interface IMongoDAO {
	public void setDBCollection(String dataBaseName,String collectionName);
	
	public void setCollection(String collectionName);
	
	public MongoCollection<Document> getCollection();
	
	public int collectionCount(Document document);
	
	public FindIterable<Document> find(Document document);
}
