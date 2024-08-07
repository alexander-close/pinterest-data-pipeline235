Project: Pinterest Database Pipeline

Description
Building a data pipeline from scratch for Pinterest-generated data.

Initial, initial setup
Git is set up by cloning the provided repository.  Sensitive files are added to the .gitignore.


Initial AWS setup
Credentials are provided for a pre-made AWS account, of which I am an EC2 user, ec2-user.  Within this account there is a pre-made S3 bucket.  The .pem file lets me access an EC2 instance from the CLI of my machine.

Inside the virtual machine, I install Kafka 2.12-2.8.1 and house it in a directory /usr/local/kafka.

I also install Java, yum update python, pip install boto3, and wget AWS MSK authenticator.

Within the AWS IAM console, I find my user ARN (noted in my creds file) and add this to my Role policy as a principal. Now the cluster can be authenticated.


Confuguring Kafka to use the MSK authenticator
In usr/local/kafka/bin, the client.properties file is already there and the security.protocol, sasl.machanism, and sasl.client variables are already set to SASL_SSL, AWS_MSK_IAM, and ..., respectively.  So only the sasl.jaas... configuration needs to be modified to the full AWS Role ARN.

Then the MSK authenticator package is installed on the EC2 instance in kafka/libs, using wget.  This .jar file is referred to globally and in all subsequent sessions by exporting the CLASSPATH variable and writing this export to the ~/.bashrc file.

Creating Topics
AWS CLI is not permissible, so within the AWS MSK console I find the Pinterest cluster and its ARN, and locate the Bootstrap server and Zookeeper connector strings.  These are noted down in my creds file.

Using the Kafka --create --topic cammand, I create three topics associated with the three incoming Pinterest data streams:  user, geo, pin.

Custom MSK plugin
I find the pre-existing S3 bucket associated with my user ID.

I download the Confluent.io AWS connector (the installation commands are saved in a some_code file on ~ of the EC2 machine:  the endpoint URL had to be updated) and copy the .tar file to the S3 bucket found above, using aws s3 cp.

Then, within the MSK console on AWS, I create a custom plugin associated to my username by locating the .tar object in the S3 bucket.

Finally, I create a Connector using this plugin again in th MSK console.  I use all the recommended config settings from the course notebook.


Creating an API gateway
An API for sending incoming Pinterest data to the MSK cluster is pre-built and named with my user ID and it's viewable in the AWS API Gateway console.

I create a resource on this API with path / and name {proxy+} to allow me to build API methods.  One method I build is the HTTP ANY method: editing the ANY integration, selecting HTTP ANY type, and using as the Endpoint URL the 'Public IPv4 DNS' address listed under my EC2 instance (noted in the creds file).

(I had to put https:// in front of the endpoint to be valid.)

I keep the rest of the parameters dafault.  Deploying the API, I make note (in the creds file) of the 'Invoke URL'.

Setting up the REST proxy on the EC2 instance
Downloading the full Confluent package to the EC2 machine (having first removed the kafka-connect-s3 directory and flushing the logs, etc, because the machine ran out of space...), I install the .tar.gz file and modify the kafka-rest.properties file.

This looks similar to the client.properties file.  I put in my Role ARN as before, and include the Bootstrap and Zookeeper strings obtained earlier.

Run the REST proxy by confluent-7.2.0/kafka-rest-start [pwd to .properties file].  Everything OK and server is listening for requests.

Python script to emulated Pinterest posting
The file user_posting_emulation.py contains a script that gathers data (as three RDB tables 'pin', 'geo', 'user'), extracts random rows, and prints them out in an infinite loop. 

My goal is to modify these to send the data for consumtion by Kafka via the REST proxy.  I do this by changing the run_infinite_post_data_loop() method so that a requests payload is defined for each extracted table row, and at the end of the operation they are all posted to the relevent Kafka topic by tweaking the Invoke URL, adding '/topics/<UserId>.pin', etc.

Important to note that, because the SQL query result (now converted to dict type) is put in a requests payload using the json.dumps() method, I neeeded to convert any datetime entries to a JSON-serialisable format first.  This was so I could just read the dictionary into the value key of the json.dumps() method.  Otherwise I would need to put in all the entries one by one.  My method is neater than this and handles futuree changes to the column values.  (Datetime format data only appeears in 'geo' and 'user' data, but the conversion is included for all three sources for consistency and to guard against future inclusion in the 'pin' source.)

Mounting the S3 bucket in Databricks
After importing the credentials (access and secret access keys) I mount the S3 bucket under a mount called <UserId>-mount.  My bucket file system is there as expected when running display(dbutils.fs.ls("/mnt/126ca3664fbb-mount/")).  Reading in the data as a dataframe goes according to plan.  Thy are titled df_pin, etc.

The code for this is stored in the mounting_notebook.ipynb file.



