package otocloud.framework.scheduler.factory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.net.URL;
import java.io.InputStream;

import org.quartz.simpl.InitThreadContextClassLoadHelper;
import org.quartz.simpl.LoadingLoaderClassLoadHelper;
import org.quartz.simpl.SimpleClassLoadHelper;
import org.quartz.simpl.ThreadContextClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;


public class OtoCloudCascadingClassLoadHelper implements ClassLoadHelper {

    private LinkedList<ClassLoadHelper> loadHelpers;

    private ClassLoadHelper bestCandidate;
    
    private List<String> jobClassPaths;
    
	public OtoCloudCascadingClassLoadHelper(List<String> jobClassPaths){
		this.jobClassPaths = jobClassPaths;
	}

    public void initialize() {
        loadHelpers = new LinkedList<ClassLoadHelper>();   
        
        jobClassPaths.forEach(jobClassPath->{
        	loadHelpers.add(new MavenJobClassLoadHelper(jobClassPath));
        });
        
        loadHelpers.add(new LoadingLoaderClassLoadHelper());
        loadHelpers.add(new SimpleClassLoadHelper());
        loadHelpers.add(new ThreadContextClassLoadHelper());
        loadHelpers.add(new InitThreadContextClassLoadHelper());
        
        for(ClassLoadHelper loadHelper: loadHelpers) {
            loadHelper.initialize();
        }
    }
    
    public void add(ClassLoadHelper classLoadHelper) {
        loadHelpers.add(classLoadHelper);
        classLoadHelper.initialize();
    }

    /**
     * Return the class with the given name.
     */
    public Class<?> loadClass(String name) throws ClassNotFoundException {

        if (bestCandidate != null) {
            try {
                return bestCandidate.loadClass(name);
            } catch (Throwable t) {
                bestCandidate = null;
            }
        }

        Throwable throwable = null;
        Class<?> clazz = null;
        ClassLoadHelper loadHelper = null;

        Iterator<ClassLoadHelper> iter = loadHelpers.iterator();
        while (iter.hasNext()) {
            loadHelper = iter.next();

            try {
                clazz = loadHelper.loadClass(name);
                break;
            } catch (Throwable t) {
                throwable = t;
            }
        }

        if (clazz == null) {
            if (throwable instanceof ClassNotFoundException) {
                throw (ClassNotFoundException)throwable;
            } 
            else {
                throw new ClassNotFoundException( String.format( "Unable to load class %s by any known loaders.", name), throwable);
            } 
        }

        bestCandidate = loadHelper;

        return clazz;
    }

    @SuppressWarnings("unchecked")
    public <T> Class<? extends T> loadClass(String name, Class<T> clazz)
            throws ClassNotFoundException {
        return (Class<? extends T>) loadClass(name);
    }
    
    /**
     * Finds a resource with a given name. This method returns null if no
     * resource with this name is found.
     * @param name name of the desired resource
     * @return a java.net.URL object
     */
    public URL getResource(String name) {

        URL result = null;

        if (bestCandidate != null) {
            result = bestCandidate.getResource(name);
            if(result == null) {
              bestCandidate = null;
            }
            else {
                return result;
            }
        }

        ClassLoadHelper loadHelper = null;

        Iterator<ClassLoadHelper> iter = loadHelpers.iterator();
        while (iter.hasNext()) {
            loadHelper = iter.next();

            result = loadHelper.getResource(name);
            if (result != null) {
                break;
            }
        }

        bestCandidate = loadHelper;
        return result;
    }

    /**
     * Finds a resource with a given name. This method returns null if no
     * resource with this name is found.
     * @param name name of the desired resource
     * @return a java.io.InputStream object
     */
    public InputStream getResourceAsStream(String name) {

        InputStream result = null;

        if (bestCandidate != null) {
            result = bestCandidate.getResourceAsStream(name);
            if(result == null) {
                bestCandidate = null;
            }
            else {
                return result;
            }
        }

        ClassLoadHelper loadHelper = null;

        Iterator<ClassLoadHelper> iter = loadHelpers.iterator();
        while (iter.hasNext()) {
            loadHelper = iter.next();

            result = loadHelper.getResourceAsStream(name);
            if (result != null) {
                break;
            }
        }

        bestCandidate = loadHelper;
        return result;
    }

    /**
     * Enable sharing of the "best" class-loader with 3rd party.
     *
     * @return the class-loader user be the helper.
     */
    public ClassLoader getClassLoader() {
        return (this.bestCandidate == null) ?
                Thread.currentThread().getContextClassLoader() :
                this.bestCandidate.getClassLoader();
    }

}
