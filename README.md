# M2 Project - IA and Semantic Web + Stream Processing

## Presentation :
The group is formed by :
- Gabriel EYAFAA JEEVAN KUMAR
- Julien MERCIER
- Paul MARQUES
- Tom REDON


## State of the project :
We worked on every questions of both parts (IA and Semantic Web + Stream Processing).
We will give some details in the next part on the last questions of the Stream Processing part.


## Difficulties and solutions :
In the first part, we didn't encountered many problems, once we understood the way java faker and apache jena were working.\n

In the second part, we had more problems to produce the end of the last Practical Work.\n
The main problem that we encountered was to do some operations on streams in the Stream Processing part.
We firstly tried to produce chained streams and that wasn't working (The first one did receive datas and send it back, but the next one couldn't handle them).
We then decided to do all our operations for the question 1 to 5 of the last practical work on one stream.
It is working just fine.\n
Another problem that we encountered is the fact that the execution of the stream seems like lazy.
We had to put a println("pfizer") to force the execution of the side effects counter (Question 5).\n
Finally, the last problem that we faced was to find a proper way to join each datas to group by age and side effects.
We decided to group them by age and then make a consumer group that would consume each topic received and allow a consumer on each partition of the topic (We realized 5 partitions for the vaccination re partition topic).


## Role repartition :
Even if Tom did the most pushes on the git, every one in the group, tried at least to help him in their own way.
They all either do a part of a question or did some research to help me find some solutions.
- Gabriel EYAFAA JEEVAN KUMAR : 2/3
- Julien MERCIER : 2/3
- Paul MARQUES : 2/3
- Tom REDON : 3/3


## Conclusion :
To conclude, we had some problems that we found solutions on, but we really think that we could have found better, easier and prettier solutions.\n
Discovering new techniques and new technologies (like Java Faker), learning how they worked and having a proper display of what we have done was truly rewarding.\n
It was really interesting to put in practice what we saw during the courses and We think that it helps us to have a better understanding of the way Kafka and the semantic web are working.\n
