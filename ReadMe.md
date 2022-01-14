# In progress: NLP & Neo4j Python demo


* The script takes text inputs (see /documents for samples), runs NLP against those inputs to find nouns and verbs, and encoding the results into a Graph

## Python application that takes documents and performs NLP analysis on them, loading the results into Neo4j. 


### Todo:

* There is a bug where you need to run the script twice to completely populate the Graph. I think I am using Neo4j Sessions improperly...
* Dockerize the Application to package all of the dependencies


## to run

python3 ./main.py


![image](https://user-images.githubusercontent.com/90913666/149382455-cc08804a-b54c-4e66-a81e-a84f6fc86182.png)
