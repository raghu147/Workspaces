all:
	rm -rf *.class out job.jar
	javac -cp "*" Alice.java
	jar cf job.jar *.class
	hadoop jar job.jar Alice alice out
