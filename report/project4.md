## Project 4 Report


### 1. Basic information
 - Team #:17
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-fall20-team-17
 - Student 1 UCI NetID:qingchy
 - Student 1 Name:Qingchuan Yang
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns). 

We won't modify "Tables" or "Columns" when managing indexes. Instead, inside createCatalog() we create another table "Indexes" to track all the indexes. 

Each tuple in the "Indexes" table has three fields: the table_name, the attribute_name and the file_name of the index. 
When we try to find indexes associated with a relation, we only search in the "Indexes" table.

### 3. Filter
- Describe how your filter works (especially, how you check the condition.)

First we read the record descriptor and the next "data" from the input iterator. Then based on the record descriptor and the data, we can move to the attribute we care about, and compare the value of the attribute with the value in the provided "Condition" to see if the data fits the "Condition." If so, we return; if not we move to the next data. 


### 4. Project
- Describe how your project works.

In the constructor, we read the "old" record descriptor and build a "new" record descriptor. Then we read "data" and for every attribute in the "new" record descriptor, we find the corresponding values in the "data", concatenate the values, and return.   


### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)

We are given two tables L and R. In the BNJ, we maintain a 

std::unordered_map<int, std::vector<std::string>> hashtable_int, if the type of the join attribute is INT or REAL, or a

std::unordered_map<std::string, std::vector<std::string>> hashtable_str, if the type of the join attribute is VARCHAR, and a 

unsigned int indicating how many bytes of tuples are already inserted.
            
For each tuple in L, we read the value of the join attribute, calculate the tuple's length, and form a std::string to maintain the tuple. Then we insert the tuple into the hashtable.

After each insertion we update the size of the hashtable and compare it with the memory requirement (numPages - 1) * pageSize (Here I use "numPages  - 1" here to compensate the overhead of hashtable and std::string.)

-If size_hashtable > memory requirement: we load all the tuples one by one from R and search the hashtable for the same key in both L and R. If so, for every L tuple in the std::vector<std::string>,
we join it with the current R tuple and write them into a temporary table. After we finish looping through all the R tuples, we go back to the previous step, until all L tuples are consumed. 

-If not, we continue inserting tuple. 

We build a iterator on the temporary table, and call the iter.getNext() inside the join.getNext().
### 6. Index Nested Loop Join
- Describe how your index nested loop join works. 

We load L tuples one by one, and for each of them, we search the B+ tree of R for the same attribute value. If there is a match, we go to the actual file to load the R tuple(s), join it with the L tuple, and write the new tuple into a temporary table.

We build a iterator on the temporary table, and call the iter.getNext() inside the join.getNext().
### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).

First we load every tuple in both L and R tables, and for each tuple, we calculate its hash value, and insert it into a newly-built table based on its hash value.

Then we do BLJ for each L sub-table and its corresponding R sub-table, inserting the output of each BLJ(sub-table L, sub-table R) pair into a temporary table.   

We build a iterator on the temporary table, and call the iter.getNext() inside the join.getNext().

Notice the hash functions for initial partition are derived from here. I also mentioned them inside code. AND, they are different from the default hash functions in std::unordered_map.

//https://stackoverflow.com/questions/7666509/hash-function-for-string . Its name is "djb2", designed by Dan Bernstein.

//https://stackoverflow.com/questions/6082915/a-good-hash-function-to-use-in-interviews-for-integer-numbers-strings

### 8. Aggregation
- Describe how your basic aggregation works.

For each Aggregation object, we maintain a current_min, current_max, current_count and current_sum. We read each tuple and update the values. After reading the whole table, we calculate the return value and return.

- Describe how your group-based aggregation works. (If you have implemented this feature)

For we define a struct of 4 float types: min, max, count and sum.

We maintain a unordered_map<int/str, the above struct>, and for each tuple insertion, we update its corresponding key. Each time we need to return, we calculate the return value, and concatenate it with the key.
  

### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.

None.


- Other implementation details:

One interesting bug I met was caused by file descriptor leak. When the number of the in-use FDs exceed 2^16, I can't open any file any more.


### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)
Project4 definitely is more time-consuming than P1 and thus worth more points.


- Feedback on the project to help improve the project. (optional)
