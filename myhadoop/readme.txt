TEAM: Nikhil, Raghu, Nephi, Karishma

This assignment submission contains

- src folder - contains all .java files
- build.xml - ant script to generate jar
- Makefile
- start-cluster.sh <number of instances> - starts clusters specified as argument
- mymapreduce.sh <class> <input path> <output path> - runs the implementation of the framework on the clusters specified above
- stop.sh - stops the running instances


Mandatory Instructions-

1. Make sure you run "aws configure" and configure your aws command line tool to have JSON output
2. Put your AWS credentials(ususally located in ~/.aws/ folder)  in a file called credentials and place it along with the root directory files.
   Make sure file is called 'credentials' ( no extensions)
3. Make sure you have ant setup
4. run start-cluster.sh, sort.sh and stop.sh with SUDO


NOTES -
- The code base pulls the appropriate JARs, packages the required files into a Jar called 'framework.jar'
- It creates the required EC-2 Private KEY, EC-2 Security Group and adds required Rules to those security groups


SAMPLE RUN:

EC2:

- ./start-cluster.sh 2 -- Starts two EC2 instances 
- ./my-mapreduce.sh <Implementation class> s3://Alice6240 s3://output6240 -- Pass the implementation class, input bucket, output bucket
where Implementation class = Alice/Median/ClusterAnalysis/MissedConnections/Prediction/Routing
- ./stop.sh -- stop instances

PSEUDO:

Make sure u specify the path and implementation class in the Makefile and run the following command. 

make pseudo -- creates output folder out containing part file with the output

CLEAN:
make remove -- reverts to original folder structure


