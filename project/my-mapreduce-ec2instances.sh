	# TO RUN: ./my-mapreduce.sh <jar-name>	
	#----------------------------------------
	output=project-bucket-cs6240-out
	input=airline6240
	intermediate=project-bucket-cs6240-int
	#---------------------------------------- 


	publicDNS=$(cat publicDNS.txt)
	listOfDNS="$publicDNS"


	portArray=()
	slaveport=19000

	#Makefile clean command
	for i in $(echo $listOfDNS | tr " " "\n")
	do
  	scp -oStrictHostKeyChecking=no  -i "key.pem" Makefile ec2-user@$i:~
	done

	#create .aws folder in every instance
	for i in $(echo $listOfDNS | tr " " "\n")
	do
 	ssh -oStrictHostKeyChecking=no -i "key.pem" ec2-user@$i "make clean"
	done

	make clean
	
	#create temp folder in every instance
	
	for i in $(echo $listOfDNS | tr " " "\n")
	do
	machines=$(($machines+1))
	done
	 


	serverNumber=0
	for i in $(echo $listOfDNS | tr " " "\n")
	do
	if [ $serverNumber -ne 0 ]
	then

	gnome-terminal -x bash -c "ssh -oStrictHostKeyChecking=no -i \"key.pem\" ec2-user@$i java -cp $1 Slave $slaveport" 
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
	
	   ssh -oStrictHostKeyChecking=no -i "key.pem" ec2-user@$i java -cp $1 Master $((machines-1)) $input $intermediate $output ${portArray[@]} 
	fi
	serverNumber=$(($serverNumber+1))
	done


