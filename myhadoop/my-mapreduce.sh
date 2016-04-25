	# TO RUN: ./my-mapreduce.sh <reflection-class-name> <inputpath> <outputpath>	TODO:pass reflection class as jar 
	#----------------------------------------
	
	intermediate=s3://testint6240
	#---------------------------------------- 


	publicDNS=$(cat publicDNS.txt)
	listOfDNS="$publicDNS"


	portArray=()
	slaveport=17023

	
	
	for i in $(echo $listOfDNS | tr " " "\n")
	do
	machines=$(($machines+1))
	done
	 
	for i in $(echo $listOfDNS | tr " " "\n")
	do
	ssh -oStrictHostKeyChecking=no -i "key.pem" ec2-user@$i "make clean"
	done
	echo "successfully transferred"

	serverNumber=0
	for i in $(echo $listOfDNS | tr " " "\n")
	do
	if [ $serverNumber -ne 0 ]
	then
	
	gnome-terminal -x bash -c "ssh -oStrictHostKeyChecking=no -i \"key.pem\" ec2-user@$i java -cp dist/framework.jar org.mapreduce.myhadoop.Slave $slaveport" 
	fi
	  serverNumber=$(($serverNumber+1))  
	  portArray+=($slaveport)
	  slaveport=$(($slaveport+1))
	done
	sleep 1
	serverNumber=0
	for i in $(echo $listOfDNS | tr " " "\n")
	do
	if [ $serverNumber -eq 0 ]
	then 
		#Args: $1= Reflection class,no.machines,$2=inputpath,$3=outputpath
	   ssh -oStrictHostKeyChecking=no -i "key.pem" ec2-user@$i java -cp  dist/framework.jar org.mapreduce.myhadoop.Master "org.mapreduce.myhadoop.$1" $((machines-1)) $2 $intermediate $3 ${portArray[@]} 
	fi
	serverNumber=$(($serverNumber+1))
	done


