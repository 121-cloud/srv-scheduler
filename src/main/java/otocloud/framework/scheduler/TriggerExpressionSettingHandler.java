/*
 * Copyright (C) 2015 121Cloud Project Group  All rights reserved.
 */
package otocloud.framework.scheduler;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;
import otocloud.framework.scheduler.factory.OtoCloudSchedulerFactory;

import otocloud.common.ActionURI;
import otocloud.common.webserver.MessageBodyConvention;
import otocloud.framework.core.HandlerDescriptor;
import otocloud.framework.core.OtoCloudBusMessage;
import otocloud.framework.core.OtoCloudComponentImpl;
import otocloud.framework.core.OtoCloudEventHandlerImpl;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;




public class TriggerExpressionSettingHandler extends OtoCloudEventHandlerImpl<JsonObject> {

	public static final String EXPRESSION_SETTING = "trigger.expression.setting";
	

	/**
	 * Constructor.
	 *
	 * @param componentImpl
	*/
	public TriggerExpressionSettingHandler(OtoCloudComponentImpl componentImpl) {
		super(componentImpl);
	}


	@Override
	public void handle(OtoCloudBusMessage<JsonObject> msg) {
		JsonObject body = msg.body();
		
		JsonObject triggerInfo = body.getJsonObject(MessageBodyConvention.HTTP_BODY);		
	
	    this.componentImpl.getVertx().<Void>executeBlocking(future -> {	
				try {
					OtoCloudSchedulerFactory schedulerFactory = ((OtoCloudSchedulerComponentImpl)this.componentImpl).getStdSchedulerFactory();				
					Scheduler scheduler = schedulerFactory.getScheduler();
					
			        TriggerKey tKey = new TriggerKey(triggerInfo.getString("name"), triggerInfo.getString("group"));
			        
			        String cronExpression = triggerInfo.getString("cron_expression");
			        
			        CronTrigger trigger = (CronTrigger)scheduler.getTrigger(tKey);
			        
			        //表达式调度构建器
			        CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);
			          
			        //按新的cronExpression表达式重新构建trigger
			        trigger = trigger.getTriggerBuilder().withIdentity(tKey)
			        		.withSchedule(scheduleBuilder).build();
			        
			        
/*					Trigger tg = scheduler.getTrigger(tKey);
			        if(tg instanceof CronTriggerImpl){
			        	((CronTriggerImpl)tg).setCronExpression(cronExpression);	        	
			        }     
			        scheduler.rescheduleJob(tKey, tg);  
			        */			        

			        //按新的trigger重新设置job执行
			        scheduler.rescheduleJob(tKey, trigger);			        
			        
					
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
		
		ActionURI uri = new ActionURI("trigger/expression", HttpMethod.PUT);
		handlerDescriptor.setRestApiURI(uri);
		
		return handlerDescriptor;		

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getEventAddress() {
		return EXPRESSION_SETTING;
	}

}
