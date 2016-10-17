package otocloud.framework.scheduler.factory;

import org.quartz.spi.ClassLoadHelper;

import java.net.URL;
import java.net.URLClassLoader;
import java.io.InputStream;


public class MavenJobClassLoadHelper implements ClassLoadHelper {

	private URLClassLoader myClassLoader;
	private String jobClassPath;
	
	public MavenJobClassLoadHelper(String jobClassPath){
		this.jobClassPath = jobClassPath;
	}
	
    public void initialize() {
		MavenClassLoader mavenClassLoader = new MavenClassLoader();
		myClassLoader = mavenClassLoader.getClassLoader(jobClassPath, Thread.currentThread().getContextClassLoader());  
    }


    public Class<?> loadClass(String name) throws ClassNotFoundException {
		return myClassLoader.loadClass(name);
    }

    @SuppressWarnings("unchecked")
    public <T> Class<? extends T> loadClass(String name, Class<T> clazz)
            throws ClassNotFoundException {
        return (Class<? extends T>) loadClass(name);
    }

    public URL getResource(String name) {
        return getClassLoader().getResource(name);
    }

    public InputStream getResourceAsStream(String name) {
        return getClassLoader().getResourceAsStream(name);
    }

    public ClassLoader getClassLoader() {
    	return myClassLoader;
    }

}
