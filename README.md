# Sample ETL pipeline for Fancy Telco Co.

### Architecture Decsription

Client stores data on SFTP server and data is updated wekly. 
Due to data scale (initial 1.66 Gb CSV) Spark Engine is chosen to manipulate data. Best commercial application of Spark
is Databrics, at the same time giving opportunity to do other required tasks:
- data ingest
- data validation
- data transformation
- simple vizualisation

Azure is chosen as a cloud provider allowing easiest integration and additional services for SFTP file management.

### Azure Setup
- Storage account is set up to host data
- Databrics workspace for computations
- Logic app for SFTP download

### General
#### SFTP Concept
SFTP is a safe file transfer protocol based on SSH. t supports the full security and authentication functionality of SSH.
SFTP replaces FTP providing all the functionality offered, but more securely and more reliably, with easier configuration.
Data is encrypted thus SFTP protects against password sniffing and man-in-the-middle attacks. It protects the integrity of 
the data using encryption and cryptographic hash functions, and autenticates both the server and the user.

File from SFTP to Azure Blob Storage is loaded using Azure logic app, having a trigger sensor to start upload once file structure changes:
![alt logic_app](img/logic_app.png)


