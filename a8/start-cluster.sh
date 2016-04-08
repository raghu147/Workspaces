make clean

#aws ec2 create-security-group --group-name sg8 --description "My security2 group"

#aws ec2 authorize-security-group-ingress --group-name sg8 --protocol tcp --port 0-65535 --cidr 0.0.0.0/0

#aws ec2 authorize-security-group-ingress --group-name sg8 --protocol tcp --port 22 --cidr 0.0.0.0/0

#aws ec2 create-key-pair --key-name key --query 'KeyMaterial' --output text > key.pem

chmod 400 key.pem


instance_id=$(aws ec2 run-instances --key key --count $1 --security-groups sg8 --instance-type t2.micro --image-id ami-c229c0a2 --output text --query 'Instances[*].InstanceId')

echo $instance_id > instance.txt

publicDNS=$(aws ec2 describe-instances --instance-id $instance_id  --query 'Reservations[].Instances[].PublicDnsName' --output text)

echo $publicDNS > publicDNS.txt

listOfDNS="$publicDNS"

echo $listOfDNS

sleep 120



#scp -i "key.pem" SampleFile.txt ec2-user@$publicDNS:~

#to do
#create a folder:required containing jar and credentials in local



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
  scp -oStrictHostKeyChecking=no -i "key.pem" ~/required/credentials ec2-user@$i:.aws
done

#copy jar to ~/home
for i in $(echo $listOfDNS | tr " " "\n")
do
  scp -oStrictHostKeyChecking=no -i "key.pem" ~/required/Instance.jar ec2-user@$i:~
done





