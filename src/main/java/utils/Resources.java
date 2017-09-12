package utils;

import java.io.File;

public class Resources {
    private String path;

    public Resources(String path) {
        this.path = path;
    }

    public File getFile(){
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(path).getFile());
    }
}