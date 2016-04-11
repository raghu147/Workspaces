machines=2
master_port=16000
port1=17321
port2=17322
output=cs6240sp16
input=cs6240sp16
clean:
	rm -f *.class
all:
	javac -cp \* *.java
master:
	java -cp .:\* Master $(machines) $(input) $(output) $(master_port) $(port1) $(port2)
slave1:
	java -cp .:\* Slave $(port1)
slave2:
	java -cp .:\* Slave $(port2)
