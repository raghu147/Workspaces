servers=2
num1=0
num2=1
ports=17000 17001

clean:
	rm *.class

all:
	javac -cp \* *.java
	
master:
	java -cp .:\* GetObject  $(servers) $(num1) $(ports)
slave:
	java -cp .:\* GetObject  $(servers) $(num2) $(ports)


