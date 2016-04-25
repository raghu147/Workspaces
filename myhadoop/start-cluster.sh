

make run

aws ec2 create-security-group --group-name sg9 --description "My security2 group"

aws ec2 authorize-security-group-ingress --group-name sg9 --protocol tcp --port 17000-17025 --cidr 0.0.0.0/0

aws ec2 authorize-security-group-ingress --group-name sg9 --protocol tcp --port 22 --cidr 0.0.0.0/0

aws ec2 create-key-pair --key-name key --query 'KeyMaterial' --output text > key.pem

chmod 400 key.pem

instanceCount=$1
instanceCount=$(($instanceCount+1))

instance_id=$(aws ec2 run-instances --key key --count $instanceCount --security-groups sg9 --instance-type t2.micro --image-id ami-c229c0a2 --output text --query 'Instances[*].InstanceId')

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

#create .aws folder in every instance
for i in $(echo $listOfDNS | tr " " "\n")
do
 ssh -oStrictHostKeyChecking=no -i "key.pem" ec2-user@$i "mkdir dist"
done


#copy credentials to ~/home/.aws
for i in $(echo $listOfDNS | tr " " "\n")
do
  scp -oStrictHostKeyChecking=no -i "key.pem" credentials ec2-user@$i:.aws
done

#copy makefile
for i in $(echo $listOfDNS | tr " " "\n")
do
  scp -oStrictHostKeyChecking=no -i "key.pem" Makefile ec2-user@$i:~
done



# Copy framework to all machines
for i in $(echo $listOfDNS | tr " " "\n")
do
  scp -oStrictHostKeyChecking=no -i "key.pem" dist/framework.jar ec2-user@$i:~/dist
done


#make clean compile in every instance
	
for i in $(echo $listOfDNS | tr " " "\n")
do
ssh -oStrictHostKeyChecking=no -i "key.pem" ec2-user@$i "make clean compile "
done
echo "successfully transferred"



