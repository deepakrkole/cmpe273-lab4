package edu.sjsu.cmpe.cache.client;

import java.util.concurrent.Future;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * Distributed cache service
 * 
 */
public class DistributedCacheService implements CacheServiceInterface {

	
	public final String cacheServerUrl;
    public  CRDTClient crdtClient;

    public DistributedCacheService(String serverUrl,CRDTClient client) {
        this.cacheServerUrl = serverUrl;
        this.crdtClient=client;
    }
    
	public String get(long key) {
		// TODO Auto-generated method stub
		Future<HttpResponse<JsonNode>> future = Unirest.get(this.cacheServerUrl + "/cache/{key}")
                .header("accept", "application/json")
                .routeParam("key", Long.toString(key))
                .asJsonAsync(new Callback<JsonNode>() {

                    public void failed(UnirestException e) {
                    	crdtClient.getFailed(cacheServerUrl);
                    	
                    }

                    public void completed(HttpResponse<JsonNode> response) {
                    	crdtClient.getResultsForServers(response, cacheServerUrl);
                    }

                    public void cancelled() {
                        System.out.println("The request has been cancelled");
                    }

                });

        return null;
	}

	public void put(long key, String value) {
		// TODO Auto-generated method stub
		Future<HttpResponse<JsonNode>> future = Unirest.put(this.cacheServerUrl + "/cache/{key}/{value}")
                .header("accept", "application/json")
                .routeParam("key", Long.toString(key))
                .routeParam("value", value)
                .asJsonAsync(new Callback<JsonNode>(){

					public void completed(
							HttpResponse<JsonNode> response) {
						System.out.println("hi this is in completed");
						
						System.out.println("value put on server"+cacheServerUrl);
						crdtClient.putForSuccess(cacheServerUrl);
						
						
					}

					public void failed(
							UnirestException e) {
						// TODO Auto-generated method stub
						crdtClient.failed(cacheServerUrl);
						
						
						
					}

					public void cancelled() {
						// TODO Auto-generated method stub
						
					}
                	
                });


		
	}

	public void delete(long key) {
		// TODO Auto-generated method stub
		 HttpResponse<JsonNode> response = null;
    	 try{
    		 
    		 response = Unirest
                     .delete(this.cacheServerUrl + "/cache/{key}")
                     .header("accept", "application/json")
                     .routeParam("key", Long.toString(key))
                     .asJson();
         } catch (UnirestException e) {
             System.err.println(e);
         }
    	 
    	 System.out.println("response is " + response);

         if (response == null || response.getCode() != 204) {
             System.out.println("Failed to delete from the cache.");
         } else {
             System.out.println("Deleted " + key + " from " + this.cacheServerUrl);
         }
		
	}

}
