LIB_PATH=/home/yaoliu/src_code/local/lib/usr/local/lib/libthrift-0.10.0.jar:/home/yaoliu/src_code/local/lib/usr/local/lib/slf4j-log4j12-1.7.12.jar:/home/yaoliu/src_code/local/lib/usr/local/lib/slf4j-api-1.7.12.jar:/home/yaoliu/src_code/local/lib/usr/local/lib/log4j-1.2.17.jar

all: clean
	thrift --gen java keyStore.thrift
	mkdir bin
	mkdir bin/classes
	javac -classpath $(LIB_PATH) -d bin/classes/ *.java gen-java/*

clean:
	rm -rf bin/
	rm -rf gen-java/


