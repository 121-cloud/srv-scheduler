/*
 * Copyright (C) 2015 121Cloud Project Group  All rights reserved.
 */
package otocloud.framework.scheduler;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;






import otocloud.common.ActionURI;
import otocloud.common.webserver.MessageBodyConvention;
import otocloud.framework.core.HandlerDescriptor;
import otocloud.framework.core.OtoCloudBusMessage;
import otocloud.framework.core.OtoCloudComponentImpl;
import otocloud.framework.core.OtoCloudEventHandlerImpl;
import otocloud.framework.scheduler.factory.MavenJobClassLoadHelper;
import otocloud.framework.scheduler.factory.OtoCloudCascadingClassLoadHelper;
import otocloud.framework.scheduler.factory.OtoCloudSchedulerFactory;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;




public class JobDeploymentHandler extends OtoCloudEventHandlerImpl<JsonObject> {

	public static final String JOB_DEP = "job.deploy"; 
	

	/**
	 * Constructor.
	 *
	 * @param componentImpl
	 */
	public JobDeploymentHandler(OtoCloudComponentImpl componentImpl) {
		super(componentImpl);
	}


	@Override
	public void handle(OtoCloudBusMessage<JsonObject> msg) {
		JsonObject body = msg.body();
		
		JsonObject jobDetailInfo = body.getJsonObject(MessageBodyConvention.HTTP_BODY);
		
		//JsonObject jobDetailInfo = jobData.getJsonObject("job");		
		//JsonObject triggerInfo = jobData.getJsonObject("trigger");
		
		String jobClassName = jobDetailInfo.getString("class");				
		String jobPackageName = jobDetailInfo.getString("package", null);
		
		JsonArray jobPackages = this.componentImpl.config().getJsonArray("job_packages", new JsonArray());
		boolean existPackage = existJobPackage(jobPackages, jobPackageName);
		
		msg.reply("ok");
		
	    this.componentImpl.getVertx().<Void>executeBlocking(future -> {	
			try {		
				
				OtoCloudSchedulerFactory schedulerFactory = ((OtoCloudSchedulerComponentImpl)this.componentImpl).getStdSchedulerFactory();				
				Scheduler scheduler = schedulerFactory.getScheduler();
				
				OtoCloudCascadingClassLoadHelper classLoadHelper = schedulerFactory.getClassLoadHelpers().get(scheduler);
				
				Class jobClass = null;
				if(jobPackageName == null)						
					jobClass = classLoadHelper.loadClass(jobClassName);
				else{
					if(!existPackage){
						MavenJobClassLoadHelper jobClassLoadHelper = new MavenJobClassLoadHelper(jobPackageName);
						classLoadHelper.add(jobClassLoadHelper);
					}
					jobClass = classLoadHelper.loadClass(jobClassName);					
				}
			
				JobDetail jobDetail= JobBuilder.newJob(jobClass)
		                .withIdentity(jobDetailInfo.getString("name"), jobDetailInfo.getString("group"))
		                .storeDurably()  // durable, 指明任务就算没有绑定Trigger仍保留在Quartz的JobStore中,  
		                .build();				
			
/*				String cronExpression = triggerInfo.getString("cron_expression");
				
				Trigger trigger= TriggerBuilder
				            .newTrigger()
				            .withIdentity(triggerInfo.getString("name"), triggerInfo.getString("group"))
				            .startNow()
				            .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
				            .build();*/				
			
				// 加入一个任务到Quartz框架中, 等待后面再绑定Trigger,  
				// 此接口中的JobDetail的durable必须为true  
				scheduler.addJob(jobDetail, false);  
		                
				//scheduler.scheduleJob(jobDetail,trigger);
								
				//msg.reply("ok");
				future.complete();
		        
			} catch (Exception e) {
			   e.printStackTrace();
	    	   future.fail(e);
	    	   //msg.fail(400, e.getMessage());
			}
			
	    }, ar->{
			if(ar.succeeded()){
				if(jobPackageName != null){					

					if(!existPackage){
		    			jobPackages.add(new JsonObject().put("name", jobPackageName));
		    			
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
	
	private boolean existJobPackage(JsonArray jobPackages, String jobPackageName){
		if(jobPackageName == null || jobPackageName.isEmpty())
			return true;
		for(Object jobPackage : jobPackages){
			if(((JsonObject)jobPackage).getString("name").equals(jobPackageName))
				return true;
		}
		return false;
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
		
		ActionURI uri = new ActionURI("job", HttpMethod.POST);
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
