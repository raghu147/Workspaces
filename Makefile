servers=2
num1=0
num2=1
ports=18003 18004

clean:
	rm *.class

all:
	javac -cp \* *.java
	
master:
	java -cp .:\* Cluster  $(servers) $(num1) $(ports)
slave:
	java -cp .:\* Cluster  $(servers) $(num2) $(ports)

