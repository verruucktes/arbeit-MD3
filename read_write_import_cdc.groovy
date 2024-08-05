import de.hybris.platform.jalo.JaloSession
import de.hybris.platform.core.Registry
import de.hybris.platform.core.TenantAwareThreadFactory
import de.hybris.platform.servicelayer.search.FlexibleSearchQuery
import de.hybris.platform.servicelayer.search.FlexibleSearchService
import groovy.json.JsonOutput
import groovy.transform.Field
import org.apache.commons.io.FileUtils
import org.apache.commons.net.ftp.FTP
import org.apache.commons.net.ftp.FTPClient

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Field FlexibleSearchService flexibleSearchService = spring.getBean("flexibleSearchService")
@Field int batchSize = 1000

// FTP server details
@Field String ftpServer = "nespresso.ftp.upload.akamai.com"
@Field String ftpUsername = "sap_cdc"
@Field String ftpPassword = "Sap_cdC@2023"
@Field String ftpDirectory = "/41710"

// List of algorithms
def algorithms = ["SHA2:sha256", "sha2:sha256", "ssha2:sha256", "SSHA2:sha256", "MD5", "NesMD5:md5", "*"]

// File to store the results
def tempFile = new File("${System.getProperty("java.io.tmpdir")}/user_details_groovy.json")
tempFile.withWriter('UTF-8') { writer ->
    writer.write('[')

    algorithms.each { al ->
        def algoConfig = al.split(":")
        def algo = algoConfig[0]
        def cdcAlgo = algoConfig.length > 1 ? algoConfig[1] : algoConfig[0]

        def offset = 0
        boolean firstRecord = true // To handle comma separation correctly

        while (true) {
            def query = """
                SELECT {account.pk}
                FROM {NesAccount AS account JOIN NesCustomer AS nescustomer ON {nescustomer.PK} = {account.customer}}
                WHERE {account.state} = '8807529087067'
                AND {account.modifiedtime} > TO_DATE('01/01/2023 00:00:00', 'dd/mm/yyyy hh24:mi:ss')
                AND {account.storeId} = 'NesStore_BE_BE'
                AND {nescustomer.passwordEncoding} = '${algo}'
                AND {nescustomer.encodedPassword} LIKE '1:%'
            """
            FlexibleSearchQuery fsQuery = new FlexibleSearchQuery(query)
            fsQuery.setStart(offset)
            fsQuery.setCount(batchSize)

            def result = flexibleSearchService.search(fsQuery).getResult()
            if (result.isEmpty()) {
                break
            }

            // Use ExecutorService for multithreading
            final ExecutorService executor = Executors.newFixedThreadPool(
                getMaxThreads(),
                new TenantAwareThreadFactory(Registry.getMasterTenant(), JaloSession.getCurrentSession())
            )
            
            result.each { account ->
                executor.execute(new UserProcessThread(writer, account, cdcAlgo, firstRecord))
                firstRecord = false // After the first record, set it to false
            }

            executor.shutdown()
            executor.awaitTermination(1, TimeUnit.DAYS)
            offset += batchSize
        }
    }

    writer.write(']')
}

// Upload the file to the FTP server
def ftpClient = new FTPClient()
try {
    ftpClient.connect(ftpServer)
    ftpClient.login(ftpUsername, ftpPassword)
    ftpClient.enterLocalPassiveMode()
    ftpClient.setFileType(FTP.BINARY_FILE_TYPE)

    FileInputStream fis = new FileInputStream(tempFile)
    boolean done = ftpClient.storeFile("${ftpDirectory}/user_details.json", fis)
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

class UserProcessThread implements Runnable {
    private Writer writer
    private Object account
    private String cdcAlgo
    private boolean firstRecord

    public UserProcessThread(Writer writer, Object account, String cdcAlgo, boolean firstRecord) {
        this.writer = writer
        this.account = account
        this.cdcAlgo = cdcAlgo
        this.firstRecord = firstRecord
    }

    public void run() {
        def userDetails = process(account, cdcAlgo)
        synchronized (writer) {
            if (!firstRecord) {
                writer.write(',')
            }
            writer.write(JsonOutput.toJson(userDetails) + '\n')
        }
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
                channel: account['entityChannel.channel.code']
            ]
        ]
        return userDetails
    }
}

// Max Thread parameter to limit number of running threads
int getMaxThreads() {
    return 1
}