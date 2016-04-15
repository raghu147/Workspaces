machines=2
master_port=16000
port1=17321
port2=17322
output=project-bucket-cs6240-out
#input=project-bucket-cs6240
input=climate6240
clean:
	rm -f *.class
all:
	javac -cp ".:lib/*" *.java
master:
	java -cp ".:lib/*" Master $(machines) $(input) $(output) $(master_port) $(port1) $(port2)
slave1:
	java -cp ".:lib/*" Slave $(port1)
slave2:
	java -cp ".:lib/*" Slave $(port2)
