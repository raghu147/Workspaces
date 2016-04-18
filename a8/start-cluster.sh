#make commands to convert class to respective jars: 
#1. Alice Word Count (alice_count.jar)
#2. Alice Median(alice_median.jar)
#3. Cluster Analysis(cluster.jar)
#4. Missed Connections (missed.jar)
#5. Prediction and Routing(prediction.jar,routing.jar)
#make clean
#make compile

#aws ec2 create-security-group --group-name sg9 --description "My security2 group"

#aws ec2 authorize-security-group-ingress --group-name sg9 --protocol tcp --port 0-65535 --cidr 0.0.0.0/0

#aws ec2 authorize-security-group-ingress --group-name sg9 --protocol tcp --port 22 --cidr 0.0.0.0/0

#aws ec2 create-key-pair --key-name key --query 'KeyMaterial' --output text > key.pem

#chmod 400 key.pem


instance_id=$(aws ec2 run-instances --key key --count $1 --security-groups sg9 --instance-type t2.micro --image-id ami-c229c0a2 --output text --query 'Instances[*].InstanceId')

echo $instance_id > instance.txt

publicDNS=$(aws ec2 describe-instances --instance-id $instance_id  --query 'Reservations[].Instances[].PublicDnsName' --output text)

echo $publicDNS > publicDNS.txt

listOfDNS="$publicDNS"

echo $listOfDNS

sleep 120



#copy publicDNS.txt to ~home
for i in $(echo $listOfDNS | tr " " "\n")
do
  scp -oStrictHostKeyChecking=no  -i "key.pem" publicDNS.txt ec2-user@$i:~
done

#create .aws folder in every instance
for i in $(echo $listOfDNS | tr " " "\n")
do
 ssh -oStrictHostKeyChecking=no -i "key.pem" ec2-user@$i "mkdir .aws"
done


#copy credentials to ~/home/.aws
for i in $(echo $listOfDNS | tr " " "\n")
do
  scp -oStrictHostKeyChecking=no -i "key.pem" credentials ec2-user@$i:.aws
done


# Copy all the jars to all machines
for i in $(echo $listOfDNS | tr " " "\n")
do
  scp -oStrictHostKeyChecking=no -i "key.pem" *.jar ec2-user@$i:~
done
echo "successfully transferred"




