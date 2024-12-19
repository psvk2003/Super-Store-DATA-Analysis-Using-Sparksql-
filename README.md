# Super store DATA Analysis Using Sparksql


1. All the analayses can be found and executed in the "orders.scala" file inside the "Source Code" folder. Just running the code will do.

2.  All the data required is inside the "Dataset" folder.

3. For visualising the results, we made use of python matplotlib.

Table of contents:
1.Prerequisites
2.Setup
3.Running the analyses
4.Analysis Results 

1. Prerequisites
•	Apache Spark installed
•	Scala installed
•	MySQL installed 
•	Python installed
•	Dataset: super_store.csv (replace with your dataset)


2. Setup
Connect the spark sql to MySQL database by:
•	In SQL workbench, enter the below query to your database after creating table and inserting column names.
LOAD DATA INFILE “path/to/data.csv”
INTO TABLE Tablename
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
•	Add these lines to your analysis file to connect it to your database.
// Database connection properties
val url = "jdbc:mysql://localhost:3306"          //replace with your connection link
val properties = new java.util.Properties()
properties.setProperty("user", "root")
properties.setProperty("password", "PASSWORD")      //replace with your username and password with your mysql username and password

3. Running the Analysis
Set up the Spark configuration by providing the appropriate Spark master URL and application name in the code.
Load the superstore dataset as csv file input.
Customize the analysis as per your requirements by modifying the code and adding additional logic or transformations.
Execute the code to perform the analysis.
the below provided are the dependencies to use for the execution of this project.
place these in the built.sbt if you are running in the intellij

dependencies:-
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-swing" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"

4. Analysis Results
After running the analysis, you'll see the results printed in the console, from analysis 1-4.
Note: Make sure to replace "path/to/data.csv" with the actual path to your dataset file before running the code.


**** NOTE :
 
-> The tables in MySQL are stored in a local system, thus creation of tables before running any files is required. Kindly use the "super_store.sql" file to create necessary tables in your local system. 

-> Every program file (.scala, .sql) loads data based on the directory. Therefore make sure to download all the files present in the "Dataset" folder and change the path of datasets in all the codes to the directory where you have them.

-> The report file is attached for easier understanding of the project objective, results and conclusion.

----------------------------------------------------------------------------------------------------------------------------


****please make sure to add the dependencies from the "build.sbt" file in the "Source Code" folder.

