package com.horizen.common;

import java.io.File;

public class Utils {
    public static void deleteDirectory(String directoryPath) {
        File directoryToBeDeleted = new File(directoryPath);
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file.getAbsolutePath());
            }
        }
        directoryToBeDeleted.delete();
    }
}
