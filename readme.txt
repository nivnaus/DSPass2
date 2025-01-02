Readme for DSP assignment 2 – Olga Oznovich 212969315 olgaoz@post.bgu.ac.il , Niv Naus 316197227 nausn@post.bgu.ac.il  

 

How to run the program: 

Set the relevant credentials in .aws/credentials 

Make sure the bucket nivolarule29122024 exists 

Make sure the following files don’t exist:  
s3://nivolarule29122024/subSums.txt 
s3://nivolarule29122024/constsW3.txt 
s3://nivolarule29122024/constsW2.txt 
s3://nivolarule29122024/output 
s3://nivolarule29122024/c0.txt 

Run Step1/App.java -> runs the cluster 

Have faith and patience :) 

 

Our programs consist of 4 steps: 

Step 1 – receives the 3 grams, and outputs all the sub-grams needed for the calculation (n1,n2..). the subs are for a 3gram w1 w2 w3:  
* * * -> for counting c0 the amount of all the words 
w1 w2 * 
* w2 * 
* w2 w3 
* * w3 
 

Mapper: receives a 3 gram and extracts w1 w2 w3 and match count. Emits all the subs with the relevant count. 

Partitioner: default.  

Reducer: receives every sub (for example “ * הלך *”) and all the counts across mappers (for example: [20, 8, 13]). Emits the sub with the sum to the file subSums. 
C0 is uploaded to s3 to a file “c0.txt” so it can be shared with the next step. 
 

Step 2 – Receives the subs and sums from subSums, and emits the trio its connected to with the constant and its value. For example, “ילד הלך לגן” c2 34. It will be the same for c0, c1, c2, n2, n3. 

C0 is downloaded from s3. 

Mapper: nothing special 

Partitioner: categorizes according to w2.hashcode 

Reducer: gathers all constants but n1, to output file constsW2 

 

Step 3 – Receives the subs and sums from subSums, and emits the trio its connected to with the constant and its value. For example, “ילד הלך לגן” n1 34. 

Mapper: nothing special 

Partitioner: categorizes according to w3.hashcode 

Reducer: gathers the constant n1, to output file constsW3 

 

Step 4 – Receives as input constsW3 and constsW2 and outputs every 3 gram with the calculated probability according to the given formula. 

Mapper: maps all the lines with constants to the same trio key 

Partitioner: categorizes according to the trio 

Reducer: parses all the constants from the values array and calculates the formula and emits the trio with the probability. 