#aws ec2 create-security-group --group-name my-sg2 --description "My security2 group"

#aws ec2 authorize-security-group-ingress --group-name my-sg2 --protocol tcp --port 22 --cidr 0.0.0.0/0

#aws ec2 create-key-pair --key-name key --query 'KeyMaterial' --output text > key.pem

#chmod 400 key.pem

rm instance.txt
rm publicDNS.txt

instance_id=$(aws ec2 run-instances --key key --count 2 --security-groups my-sg2 --instance-type t1.micro --image-id ami-9dbea4fc --output text --query 'Instances[*].InstanceId')

echo $instance_id > instance.txt

publicDNS=$(aws ec2 describe-instances --instance-id $instance_id  --query 'Reservations[].Instances[].PublicDnsName' --output text)

echo $publicDNS > publicDNS.txt

listOfDNS="\"$publicDNS\""

echo $listOfDNS

#scp -i "key.pem" SampleFile.txt ubuntu@$publicDNS:~

for i in $(echo $listOfDNS | tr " " "\n")
do
  scp -i "key.pem" SampleFile.txt ubuntu@$i:~
done

