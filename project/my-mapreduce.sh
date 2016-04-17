
machines=1
master_port=20010

output=project-bucket-cs6240-out
#input=climate6240
intermediate=intermediate6240
input=airline6240
portArray=()
slaveport=25018

javac -cp ".:lib/*" *.java 


#for i in $machines
for (( i=1; i <= $machines; i++ ))
do

#gnome-terminal -e "bash -c java -cp \".:lib/*\" Slave $slaveport" 
gnome-terminal -x bash -c "java -cp \".:lib/*\" Slave $slaveport" 
 slaveport=$(($slaveport+1))
  portArray+=($slaveport)
done

echo ${portArray[@]}
#echo $machines $input $intermediate $output $master_port ${portArray[@]}
#gnome-terminal -e "bash -c java -cp \".:lib/*\" Master $machines $input $intermediate $output $master_port ${portArray[@]}"
sleep 2
java -cp ".:lib/*" Master $machines $input $intermediate $output $master_port ${portArray[@]}
