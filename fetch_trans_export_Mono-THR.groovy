import de.hybris.platform.servicelayer.search.FlexibleSearchQuery
import de.hybris.platform.servicelayer.search.FlexibleSearchService
import groovy.json.JsonOutput
import groovy.transform.Field
import org.apache.commons.io.FileUtils
import org.apache.commons.net.ftp.FTP
import org.apache.commons.net.ftp.FTPClient

import java.io.FileInputStream
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Field int batchSize = 100000

// FTP server details
@Field String ftpServer = "***"
@Field String ftpUsername = "***"
@Field String ftpPassword = "***"
@Field String ftpDirectory = "***"

// File to store the results
@Field String tempFileName = "user_details_groovy.json"
@Field File tempFile = new File(System.getProperty("java.io.tmpdir"), tempFileName)

// List of algorithms
@Field List<String> algorithms = ["SHA2:sha256", "sha2:sha256", "ssha2:sha256", "SSHA2:sha256", "MD5", "NesMD5:md5", "*"]

tempFile.withWriter('UTF-8') { writer ->
    writer.write('[')
    boolean firstRecord = true // To handle comma separation correctly

    algorithms.each { al ->
        def algoConfig = al.split(":")
        def algo = algoConfig[0]
        def cdcAlgo = algoConfig.length > 1 ? algoConfig[1] : algoConfig[0]
        def offset = 0

        while (true) {
            def query = """
                SELECT {account.pk}
                FROM {tnuoccAseN AS account JOIN remotsuCseN AS remotsuCseN ON {remotsuCseN.PK} = {account.customer}}
                WHERE {account.state} = '8807529087067'
                AND {account.modifiedtime} > TO_DATE('01/01/2023 00:00:00', 'dd/mm/yyyy hh24:mi:ss')
                AND {account.storeId} = '_CH_CH'
                AND {remotsuCseN.passwordEncoding} = '${algo}'
                AND {remotsuCseN.encodedPassword} LIKE '1:%'
            """

            FlexibleSearchQuery fsQuery = new FlexibleSearchQuery(query)
            fsQuery.setStart(offset)
            fsQuery.setCount(batchSize)

            def result = flexibleSearchService.search(fsQuery).getResult()
            if (!result.isEmpty()) {
                println "Found: ${result.size()} account(s) with algo: ${algo} .. start processing account"
                
                result.each { account ->
                    def userDetails = process(account, cdcAlgo)
                    
                    if (!firstRecord) {
                        writer.write(',')
                    }
                    
                    writer.write(JsonOutput.toJson(userDetails) + '\n')
                    firstRecord = false // After the first record, set it to false
                }
                
                offset += batchSize
            } else {
                println "0 account found for ${algo}, skipping ..."
                break
            }
        }
    }

    writer.write(']')
}

private Map process(Object account, String cdcAlgo) {
    def customer = account.customer
    def userPK = customer.pk.toString()
    def userPass = customer.encodedPassword
    def finalPass = userPass
    def email = customer.uid

    def userDetails = [
        Uid: userPK,
        isRegistered: true,
        isVerified: true,
        loginIDs: [
            emails: [email]
        ],
        profile: [
            email: email
        ],
        password: [
            hashSettings: [
                algorithm: cdcAlgo
            ],
            hashedPassword: finalPass
        ],
        data: [
            channel: account.entityChannel.channel.code
        ]
    ]

    return userDetails
}

// Port for FTP (default is 21)
int ftpPort = 21

// Check FTP server connectivity using socket connection
boolean isReachable = false
Socket socket = new Socket()
try {
    socket.connect(new InetSocketAddress(ftpServer, ftpPort), 5000) // Timeout in milliseconds
    isReachable = true
    println "Connection to FTP server (${ftpServer}:${ftpPort}) is successful."
} catch (IOException e) {
    println "Connection error: " + e.message
} finally {
    try {
        socket.close()
    } catch (IOException e) {
        println "Error closing socket: " + e.message
        e.printStackTrace()
    }
}

if (isReachable) {
    // Upload the file to the FTP server
    def ftpClient = new FTPClient()
    try {
        ftpClient.connect(ftpServer)
        ftpClient.login(ftpUsername, ftpPassword)
        ftpClient.enterLocalPassiveMode()
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE)

        FileInputStream fis = new FileInputStream(tempFile)
        boolean done = ftpClient.storeFile("${ftpDirectory}/${tempFileName}", fis)
        fis.close()

        if (done) {
            println "The file was uploaded successfully."
        } else {
            println "Failed to upload the file."
        }
    } catch (Exception ex) {
        println "Error: " + ex.message
        ex.printStackTrace()
    } finally {
        try {
            if (ftpClient.isConnected()) {
                ftpClient.logout()
                ftpClient.disconnect()
            }
        } catch (IOException ex) {
            ex.printStackTrace()
        }
    }
}
