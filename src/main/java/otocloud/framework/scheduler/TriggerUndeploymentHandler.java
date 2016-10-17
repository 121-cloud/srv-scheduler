/*
 * Copyright (C) 2015 121Cloud Project Group  All rights reserved.
 */
package otocloud.framework.scheduler;

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




public class TriggerUndeploymentHandler extends OtoCloudEventHandlerImpl<JsonObject> {

	public static final String TRIGGER_UNDEP = "trigger.undeploy";
	

	/**
	 * Constructor.
	 *
	 * @param componentImpl
	 */
	public TriggerUndeploymentHandler(OtoCloudComponentImpl componentImpl) {
		super(componentImpl);
	}


	@Override
	public void handle(OtoCloudBusMessage<JsonObject> msg) {
		JsonObject body = msg.body();
		
		JsonObject triggerInfo = body.getJsonObject(MessageBodyConvention.HTTP_QUERY);		
	
	    this.componentImpl.getVertx().<Void>executeBlocking(future -> {	
				try {
					
					OtoCloudSchedulerFactory schedulerFactory = ((OtoCloudSchedulerComponentImpl)this.componentImpl).getStdSchedulerFactory();				
					Scheduler scheduler = schedulerFactory.getScheduler();
					
			        TriggerKey tKey = new TriggerKey(triggerInfo.getString("name"), triggerInfo.getString("group"));	
			        
			        scheduler.pauseTrigger(tKey);// 停止触发器
			        scheduler.unscheduleJob(tKey); // 移除触发器
					
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
		
		ActionURI uri = new ActionURI("", HttpMethod.DELETE);
		handlerDescriptor.setRestApiURI(uri);
		
		return handlerDescriptor;		

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getEventAddress() {
		return TRIGGER_UNDEP;
	}

}
