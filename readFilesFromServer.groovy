import java.nio.file.Files
import java.nio.file.Paths

// Define the file path
String filePath = System.getProperty("java.io.tmpdir")+"/user_details_groovy.json"


// Read the file content
try {
    List<String> lines = Files.readAllLines(Paths.get(filePath))
    lines.each { line ->
        println line
    }
} catch (IOException e) {
    println "Error reading file: " + e.message
    e.printStackTrace()
}
