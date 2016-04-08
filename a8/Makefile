
compile:
	
	mkdir lib
	wget https://www.dropbox.com/s/j92w2h2wkrhtqpl/aws-java-sdk-1.10.65.jar
	wget https://www.dropbox.com/s/y8uwfsu4n5m3d7x/commons-logging-1.1.3.jar
	wget https://www.dropbox.com/s/ownwatesagg4ama/commons-codec-1.6.jar
	wget https://www.dropbox.com/s/3pqcdwf8m0k1kzd/httpclient-4.3.6.jar
	wget https://www.dropbox.com/s/si5zwxdr0ecxloh/httpcore-4.3.3.jar
	wget https://www.dropbox.com/s/vuh5ni5gmy4rg8c/jackson-annotations-2.5.0.jar
	wget https://www.dropbox.com/s/qa4hq2unjlsx9mq/jackson-core-2.5.3.jar
	wget https://www.dropbox.com/s/hqak1jygo1tq22r/jackson-databind-2.5.3.jar
	wget https://www.dropbox.com/s/x3u9mlkjhgvgf46/javax.mail-api-1.4.6.jar
	wget https://www.dropbox.com/s/gtd9blwwz0p5abr/joda-time-2.8.1.jar
	
	javac -cp "*" Cluster.java Server.java
	mv *.jar lib
	jar cfm Climate.jar MANIFEST.MF *.class
	#java -cp  Climate.jar:lib/* Cluster 1 0 cs6240sp16 s3output 7777 11000


clean:
	rm -f *.class
	rm -f *.jar
	rm -f publicDNS.txt
	rm -f instance.txt
	rm -f key.pem
	rm -rf lib
