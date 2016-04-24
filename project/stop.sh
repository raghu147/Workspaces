instanceList="$(cat instance.txt)"
echo $instanceList

for i in $(echo $instanceList | tr " " "\n")
do
  aws ec2 stop-instances --instance-ids $i
done
