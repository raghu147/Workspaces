#----------------------------------------
output=project-bucket-cs6240-out
input=all-data-cs6240
intermediate=project-bucket-cs6240-int
#----------------------------------------

machines=2
master_port=16000

portArray=()
slaveport=25018

make clean all

for (( i=1; i <= $machines; i++ ))
do
gnome-terminal -x bash -c "java -cp \".:lib/*\" Slave $slaveport" 
  
  portArray+=($slaveport)
  slaveport=$(($slaveport+1))
done

sleep 1
java -cp ".:lib/*" Master $machines $input $intermediate $output $master_port ${portArray[@]}
