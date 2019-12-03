JOB_NAME=TP2_FrequencyCollection
CURRENT_DIRECTORY=~/cs435_workspace/${JOB_NAME}/src
TESTFILE=/TP/JoinedDataset
DFS_RESULT=/TP/result_JoinedDataset
LOCAL_RESULT=${CURRENT_DIRECTORY}/result_JoinedDataset
HGET=-${HADOOP_HOME}/bin/hadoop fs -get
HJAR=${HADOOP_HOME}/bin/hadoop jar ${JOB_NAME}.jar ${JOB_NAME}
HREMOVE=-${HADOOP_HOME}/bin/hadoop fs -rm -R

all: clean build run get

clean:
	clear
	-rm *.class
	-rm -R ${LOCAL_RESULT}
	-rm -R ${CURRENT_DIRECTORY}/CleanedJoinedDataset
	${HREMOVE} ${DFS_RESULT}
	${HREMOVE} /user/joshtb/CleanedJoinedDataset

build:
	${HADOOP_HOME}/bin/hadoop com.sun.tools.javac.Main ${JOB_NAME}.java
	jar cf ${CURRENT_DIRECTORY}/${JOB_NAME}.jar ${JOB_NAME} *.class

run:
	${HJAR} ${TESTFILE} ${DFS_RESULT}

get:
	${HGET} /user/joshtb/CleanedJoinedDataset ${CURRENT_DIRECTORY}/CleanedJoinedDataset
	#${HGET} /user/joshtb/ProfileAJob2 ${CURRENT_DIRECTORY}/ProfileAJob2
	${HGET} ${DFS_RESULT} ${LOCAL_RESULT}




