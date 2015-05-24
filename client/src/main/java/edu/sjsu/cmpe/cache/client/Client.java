package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
			System.out.println("Write operation...");
	        CRDTClient crdtClient = new CRDTClient();

	       
	      boolean result = crdtClient.put(1, "a");
	        System.out.println("put (1 => a)" );

	        Thread.sleep(30000);

	        crdtClient.put(1, "b");
	        System.out.println("put (1 => b)" );

	        Thread.sleep(30000);
	        String value = crdtClient.get(1);
	        
	        System.out.println("Step 3: get(1) => " + value);

	        System.out.println("Exiting Client...");
    }

}
