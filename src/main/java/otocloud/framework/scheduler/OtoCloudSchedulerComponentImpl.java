/*
 * Copyright (C) 2015 121Cloud Project Group  All rights reserved.
 */
package otocloud.framework.scheduler;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import otocloud.framework.core.OtoCloudComponentImpl;
import otocloud.framework.core.OtoCloudEventHandlerRegistry;
import otocloud.framework.scheduler.factory.OtoCloudSchedulerFactory;

/**
 * TODO: DOCUMENT ME!
 * @date 2015年6月20日
 * @author lijing@yonyou.com
 */
public class OtoCloudSchedulerComponentImpl extends OtoCloudComponentImpl {

	private OtoCloudSchedulerFactory schedulerFactory ;
	
	public OtoCloudSchedulerFactory getStdSchedulerFactory() {
		return schedulerFactory;
	}

	@Override 
    public void start(Future<Void> startFuture) throws Exception {

		Future<Void> innerFuture = Future.future();
		super.start(innerFuture);;
		innerFuture.setHandler(ret -> {
    		if(ret.failed()){
    			startFuture.fail(ret.cause());
    		}else{
    			JsonArray jobs = config().getJsonArray("job_packages", null);
    			List<String> jobList = new ArrayList<String>();
    			if(jobs != null ){
    				jobs.forEach(item->{
    					jobList.add(((JsonObject)item).getString("name"));
    				});
    			}
    			vertx.<Void>executeBlocking(future -> {
    				try{    					
    			        Properties props = new Properties();        
    					JsonObject schedulerCfg = config().getJsonObject("init");	
    					if(schedulerCfg != null){
    						schedulerCfg.forEach(cfgItem->{
    							String keyString = cfgItem.getKey();		
    							props.setProperty(keyString, (String)cfgItem.getValue());    										
    						});	
    					}
    					
    					schedulerFactory = new OtoCloudSchedulerFactory(props, jobList);   
    					
    					Scheduler scheduler = schedulerFactory.getScheduler();
    					scheduler.getContext().put("vertx", this.vertx);
    					scheduler.getContext().put("otocloud_component", this);
    					
    					scheduler.start();
    		            
    					startFuture.complete();
    		            future.complete();
    		                  
    		       } catch (Exception ex) {       
     		    	   future.fail(ex);
     		    	   startFuture.fail(ex);
     		       }   			

    		    }, ar->{
    		    	
    		    });
    			
    		}
		});  
		
	}
	
	
	@Override
	public void stop(Future<Void> stopFuture) throws Exception {
		Future<Void> innerFuture = Future.future();
		super.stop(innerFuture);;
		innerFuture.setHandler(ret -> {
    		if(ret.failed()){
    			stopFuture.fail(ret.cause());
    		}else{
    			
    		    vertx.<Void>executeBlocking(future -> {
    				try 
    		        {	
    					Scheduler sched = schedulerFactory.getScheduler();
    				    if (!sched.isShutdown()) {
    				        sched.shutdown();
    				    }
    				    
    		            stopFuture.complete();
    		            future.complete();
    		                  
    		       } catch (SchedulerException ex) {       
    		    	   future.fail(ex);
    		    	   stopFuture.fail(ex);
    		       }

    		    }, ar->{
    		    	
    		    });
    			
    		}
		});  
	}

	@Override
	public String getName() {		
		return "scheduler";
	}

	@Override
	public List<OtoCloudEventHandlerRegistry> registerEventHandlers() {
		List<OtoCloudEventHandlerRegistry> ret = new ArrayList<OtoCloudEventHandlerRegistry>();
		
		JobDeploymentHandler jobDeploymentHandler = new JobDeploymentHandler(this);
		ret.add(jobDeploymentHandler);
		
		JobUndeploymentHandler jobUndeploymentHandler = new JobUndeploymentHandler(this);
		ret.add(jobUndeploymentHandler);	
		
		TriggerDeploymentHandler triggerDeploymentHandler = new TriggerDeploymentHandler(this);
		ret.add(triggerDeploymentHandler);
		
		TriggerQueryHandler jobQueryHandler = new TriggerQueryHandler(this);
		ret.add(jobQueryHandler);
		
		TriggerExpressionGetHandler triggerExpressionGetHandler = new TriggerExpressionGetHandler(this);
		ret.add(triggerExpressionGetHandler);
		
		TriggerExpressionSettingHandler triggerExpressionSettingHandler = new TriggerExpressionSettingHandler(this);
		ret.add(triggerExpressionSettingHandler);
	
		TriggerUndeploymentHandler triggerUndeploymentHandler = new TriggerUndeploymentHandler(this);
		ret.add(triggerUndeploymentHandler);
		
		return ret;
	}
	
}
