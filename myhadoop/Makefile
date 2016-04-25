machines=1
master_port=19000
port1=17328

class=MissedConnections
output=s3://output6240
#input=testingnikcs6240
input=s3://airline6240
intermediate=s3://testint6240
pinp=inp
pintermediate=int
pout=out

run: clean compile ant

 
remove:
	rm -f *.class
	rm -rf lib	
	rm -rf mapper-temp
	rm -rf reducer-temp
	rm -rf $(pintermediate)
	rm -rf $(pout)
	rm -f key.pem
	rm -f instance.txt
	rm -f publicDNS.txt
	rm -rf bin
	rm -rf dist
	


 
clean:
	rm -f *.class
	#rm -rf lib	
	rm -rf mapper-temp
	rm -rf reducer-temp
	rm -rf $(pintermediate)
	rm -rf $(pout)
	mkdir -p lib	
	mkdir $(pout)
	mkdir $(pintermediate)
	mkdir mapper-temp
	mkdir reducer-temp


compile:

	wget https://www.dropbox.com/s/j92w2h2wkrhtqpl/aws-java-sdk-1.10.65.jar -P lib/ 
	wget https://www.dropbox.com/s/y8uwfsu4n5m3d7x/commons-logging-1.1.3.jar -P lib/ 
	wget https://www.dropbox.com/s/ownwatesagg4ama/commons-codec-1.6.jar -P lib/ 
	wget https://www.dropbox.com/s/3pqcdwf8m0k1kzd/httpclient-4.3.6.jar -P lib/ 
	wget https://www.dropbox.com/s/si5zwxdr0ecxloh/httpcore-4.3.3.jar -P lib/ 
	wget https://www.dropbox.com/s/vuh5ni5gmy4rg8c/jackson-annotations-2.5.0.jar -P lib/ 
	wget https://www.dropbox.com/s/qa4hq2unjlsx9mq/jackson-core-2.5.3.jar  -P lib/ 
	wget https://www.dropbox.com/s/hqak1jygo1tq22r/jackson-databind-2.5.3.jar -P lib/ 
	wget https://www.dropbox.com/s/x3u9mlkjhgvgf46/javax.mail-api-1.4.6.jar -P lib/ 
	wget https://www.dropbox.com/s/gtd9blwwz0p5abr/joda-time-2.8.1.jar -P lib/ 



ant:
	ant

	

#PSEUDO
master:
	java -cp dist/framework.jar org.mapreduce.myhadoop.Master org.mapreduce.myhadoop.$(class) $(machines) $(input) $(intermediate) $(output) $(master_port) $(port1)
slave:
	gnome-terminal -x bash -c "java -cp dist/framework.jar org.mapreduce.myhadoop.Slave $(port1)" 
pseudo:	clean ant

	java -cp dist/framework.jar org.mapreduce.myhadoop.Pseudo org.mapreduce.myhadoop.$(class) $(pinp) $(pintermediate) $(pout)
	cat $(pout)/* >> partfile
	rm $(pout)/* 
	mv partfile $(pout)
