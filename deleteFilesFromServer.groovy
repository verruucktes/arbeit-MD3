import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.Path

// Define the file path
String filePath = System.getProperty("java.io.tmpdir")+"/user_details_groovy.json"

// Delete the file
try {
    Path path = Paths.get(filePath)
    if (Files.exists(path)) {
        Files.delete(path)
        println "File deleted successfully: ${filePath}"
    } else {
        println "File not found: ${filePath}"
    }
} catch (IOException e) {
    println "Error deleting file: " + e.message
    e.printStackTrace()
}
