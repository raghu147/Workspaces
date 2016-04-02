
input=$1
output=$2

publicDNS=$(cat publicDNS.txt)
listOfDNS="$publicDNS"
count=0
#echo ${#source[@]}
for i in $(echo $listOfDNS | tr " " "\n")
do
count=$(($count+1))
done

if [ $count -eq 2 ]
then
ports="8000 8001"
fi

if [ $count -eq 8 ]
then
ports="8000 8001 8002 8003 8004 8005 8006 8007"
fi


serverNumber=0
for i in $(echo $listOfDNS | tr " " "\n")
do
if [ $serverNumber -ne 0 ]
then 
   ssh -oStrictHostKeyChecking=no -i "key.pem" ec2-user@$i "java -jar Climate.jar  $count $serverNumber $input $output 7777 $ports"
fi
serverNumber=$(($serverNumber+1))
done

serverNumber=0
for i in $(echo $listOfDNS | tr " " "\n")
do
if [ $serverNumber -eq 0 ]
then  
   ssh -oStrictHostKeyChecking=no -i "key.pem" ec2-user@$i "java -jar Climate.jar  $count $serverNumber $input $output 7777 $ports"
fi
serverNumber=$(($serverNumber+1))
done
