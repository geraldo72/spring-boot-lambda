package com.luisgr.springlambda;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * A sample AWS Lambda function to receive records from one Kinesis stream.
 */
@SpringBootApplication
public class SpringLambdaApp implements RequestHandler<KinesisEvent, Void>
{
	private static final Function<String[], ApplicationContext> cache = Memoizer.memoize(SpringLambdaApp::getApplicationContext);
    
    public static ApplicationContext getApplicationContext(String[] args) {
    	return SpringApplication.run(SpringLambdaApp.class, args);
	}
    
	public static void main(String[] args) {
		cache.apply(args);
	}    
    
	/**
	 * Receive Kinesis records with Json data 
	 */
    public Void handleRequest(KinesisEvent input, Context context)
    {
        LambdaLogger logger = context.getLogger();
        logger.log("Received " + input.getRecords().size() + " raw Kinesis records.");
        
        try
        {
        	Gson g = new Gson();
        	
        	List<JsonElement> putRecordsRequestEntryList = input.getRecords().parallelStream()
                    .map(r -> g.fromJson(new String(r.getKinesis().getData().array()), JsonElement.class))
                    .collect(Collectors.toList());
        	logger.log(putRecordsRequestEntryList.toString());
        } 
        catch (Exception e) 
        {
            logger.log("Lambda function encountered fatal error: " + e.getMessage());
        }

        return null;
    }
}