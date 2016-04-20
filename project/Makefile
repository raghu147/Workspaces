machines=1
master_port=16000
port1=17321
port2=17322
class=MissedConnections
output=outputnikcs6240
input=testingnikcs6240
intermediate=internikcs6240
run: clean all
 
clean:
	rm -f *.class
	rm -f mapper-temp/*
	rm -f reducer-temp/*
all:
	javac -cp ".:lib/*" *.java
master:
	java -cp ".:lib/*" Master $(class) $(machines) $(input) $(intermediate) $(output) $(master_port) $(port1)
slave1:
	java -cp ".:lib/*" Slave $(port1)
slave2:
	java -cp ".:lib/*" Slave $(port2)
