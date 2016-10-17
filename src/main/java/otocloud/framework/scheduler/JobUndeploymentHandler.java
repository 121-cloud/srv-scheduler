/*
 * Copyright (C) 2015 121Cloud Project Group  All rights reserved.
 */
package otocloud.framework.scheduler;

import org.quartz.JobKey;
import org.quartz.Scheduler;

import otocloud.common.ActionURI;
import otocloud.common.webserver.MessageBodyConvention;
import otocloud.framework.core.HandlerDescriptor;
import otocloud.framework.core.OtoCloudBusMessage;
import otocloud.framework.core.OtoCloudComponentImpl;
import otocloud.framework.core.OtoCloudEventHandlerImpl;
import otocloud.framework.scheduler.factory.OtoCloudSchedulerFactory;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;




public class JobUndeploymentHandler extends OtoCloudEventHandlerImpl<JsonObject> {

	public static final String JOB_DEP = "job.undeploy"; 
	

	/**
	 * Constructor.
	 *
	 * @param componentImpl
	 */
	public JobUndeploymentHandler(OtoCloudComponentImpl componentImpl) {
		super(componentImpl);
	}


	@Override
	public void handle(OtoCloudBusMessage<JsonObject> msg) {
		JsonObject body = msg.body();
		
		JsonObject jobDetailInfo = body.getJsonObject(MessageBodyConvention.HTTP_QUERY);
        JobKey jobKey = new JobKey(jobDetailInfo.getString("name"), jobDetailInfo.getString("group"));
        String jobPackageName = jobDetailInfo.getString("package", null);
		
	    this.componentImpl.getVertx().<Void>executeBlocking(future -> {	
			try {								
		        OtoCloudSchedulerFactory schedulerFactory = ((OtoCloudSchedulerComponentImpl)this.componentImpl).getStdSchedulerFactory();				
				Scheduler scheduler = schedulerFactory.getScheduler();
				
				scheduler.deleteJob(jobKey);
				
				msg.reply("ok");
				future.complete();
		        
			} catch (Exception e) {
			   e.printStackTrace();
	    	   future.fail(e);
	    	   msg.fail(400, e.getMessage());
			}
	
	    }, ar->{
			if(ar.succeeded()){
				if(jobPackageName != null){
					
	    			JsonArray jobPackages = this.componentImpl.config().getJsonArray("job_packages", new JsonArray());
	    			JsonObject jobPackage = getJobPackage(jobPackages, jobPackageName);
	    			if(jobPackage != null){
	    			
		    			jobPackages.remove(jobPackage);
		    			
		    			Future<Void> saveFuture = Future.future();
		    			this.componentImpl.saveConfig(saveFuture);
		    			saveFuture.setHandler(handler->{
		    				if(handler.succeeded()){	    					
		    				}else{
		    					Throwable err = handler.cause();
		    					err.printStackTrace();
		    				}
		    			});
	    			}
				}			
				
			}else{
				Throwable err = ar.cause();
				err.printStackTrace();
			}
	    });
		
	}	
	
	private JsonObject getJobPackage(JsonArray jobPackages, String jobPackageName){
		for(Object jobPackage : jobPackages){
			if(((JsonObject)jobPackage).getString("name").equals(jobPackageName))
				return (JsonObject)jobPackage;
		}
		return null;
	}

	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public HandlerDescriptor getHanlderDesc() {		
		
		HandlerDescriptor handlerDescriptor = super.getHanlderDesc();
		
		//参数
/*		List<ApiParameterDescriptor> paramsDesc = new ArrayList<ApiParameterDescriptor>();
		paramsDesc.add(new ApiParameterDescriptor("targetacc",""));		
		paramsDesc.add(new ApiParameterDescriptor("soid",""));		
		handlerDescriptor.setParamsDesc(paramsDesc);	*/
		
		ActionURI uri = new ActionURI("job", HttpMethod.DELETE);
		handlerDescriptor.setRestApiURI(uri);
		
		return handlerDescriptor;		

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getEventAddress() {
		return JOB_DEP;
	}

}
