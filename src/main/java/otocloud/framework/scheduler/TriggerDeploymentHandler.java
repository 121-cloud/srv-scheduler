/*
 * Copyright (C) 2015 121Cloud Project Group  All rights reserved.
 */
package otocloud.framework.scheduler;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import otocloud.framework.scheduler.factory.OtoCloudSchedulerFactory;

import otocloud.common.ActionURI;
import otocloud.common.webserver.MessageBodyConvention;
import otocloud.framework.core.HandlerDescriptor;
import otocloud.framework.core.OtoCloudBusMessage;
import otocloud.framework.core.OtoCloudComponentImpl;
import otocloud.framework.core.OtoCloudEventHandlerImpl;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;




public class TriggerDeploymentHandler extends OtoCloudEventHandlerImpl<JsonObject> {

	public static final String TRIGGER_DEP = "trigger.deploy"; 
	

	/**
	 * Constructor.
	 *
	 * @param componentImpl
	 */
	public TriggerDeploymentHandler(OtoCloudComponentImpl componentImpl) {
		super(componentImpl);
	}


	@Override
	public void handle(OtoCloudBusMessage<JsonObject> msg) {
		JsonObject body = msg.body();
		
		JsonObject jobData = body.getJsonObject(MessageBodyConvention.HTTP_BODY);
		
		JsonObject jobDetailInfo = jobData.getJsonObject("job");		
		JsonObject triggerInfo = jobData.getJsonObject("trigger");
		JsonObject jobDataMap = jobData.getJsonObject("job_context", null);
		
	    this.componentImpl.getVertx().<Void>executeBlocking(future -> {	
			try {			
				JobKey jobKey = new JobKey(jobDetailInfo.getString("name"), jobDetailInfo.getString("group"));
				
				String cronExpression = triggerInfo.getString("cron_expression");
				
				TriggerBuilder<Trigger> triggerBuilder = TriggerBuilder.newTrigger()
			            .withIdentity(triggerInfo.getString("name"), triggerInfo.getString("group"))
			            .forJob(jobKey); //使用scheduleJob(Trigger)加入的Trigger必须指明JobName与JobGroup  
				
				if(jobDataMap != null){
					jobDataMap.forEach(item->{					    
						Object value = item.getValue();
						if(value instanceof String){
							triggerBuilder.usingJobData(item.getKey(), (String)value);
						}else if(value instanceof Integer){
							triggerBuilder.usingJobData(item.getKey(), (Integer)value);
						}else if(value instanceof Long){
							triggerBuilder.usingJobData(item.getKey(), (Long)value);
						}else if(value instanceof Float){
							triggerBuilder.usingJobData(item.getKey(), (Float)value);
						}else if(value instanceof Double){
							triggerBuilder.usingJobData(item.getKey(), (Double)value);
						}else if(value instanceof Boolean){
							triggerBuilder.usingJobData(item.getKey(), (Boolean)value);
						}						
					});
				}
				
				Trigger trigger= triggerBuilder.startNow()
				            .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
				            .build();				
				
				OtoCloudSchedulerFactory schedulerFactory = ((OtoCloudSchedulerComponentImpl)this.componentImpl).getStdSchedulerFactory();				
				Scheduler scheduler = schedulerFactory.getScheduler();
				
				scheduler.scheduleJob(trigger); 
		                
				//scheduler.scheduleJob(jobDetail,trigger);
				
/*				if (!scheduler.isShutdown()) {
					scheduler.start();
			    }*/
				
				msg.reply("ok");
				future.complete();
		        
			} catch (Exception e) {
			   e.printStackTrace();
	    	   future.fail(e);
	    	   msg.fail(400, e.getMessage());
			}
	
	    }, ar->{
	    	
	    });
		
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
		
		ActionURI uri = new ActionURI("trigger", HttpMethod.POST);
		handlerDescriptor.setRestApiURI(uri);
		
		return handlerDescriptor;		

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getEventAddress() {
		return TRIGGER_DEP;
	}

}
