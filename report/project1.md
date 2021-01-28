## Project 1 Report


### 1. Basic information
 - Team #:17
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-fall20-team-17
 - Student 1 UCI NetID:qingchy
 - Student 1 Name:Qingchuan Yang
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Internal Record Format
- Show your record format design.

record 
= directory + actual data
= number of attributes(2 bytes) + offset to the end of the first attribute(2 bytes) + offset to the end of the second attribute(2 bytes) + ... + actual data 

Note: the data is contiguous.

- Describe how you store a null field.

The offset is set to -1 for the null field in the directory. No data is included in the "actual data"(mentioned above) area.   

- Describe how you store a VarChar field.

It simply follows the rule mentioned above.  
We can compute the length of the actual data by doing (Offset_current_record - Offset_previous_record).

- Describe how your record design satisfies O(1) field access.

As we know the position p in which the field is stored, we can calculate the the beginning and the end of the record:
To calculate the beginning, we can find the offset of the previous record in the directory by doing 2 + (p - 2) * 2 to calculate where the offset is located and read this value. 
To calculate the end, we do 2 + (p - 1) * 2 and find the offset.
The above procedures take O(1).
With these two values, we can retrieve the data in O(1).


### 3. Page Format
- Show your page format design.

page
= actual records + empty space + descriptor for each records(4 bytes for each record) + page descriptor(4 bytes)


- Explain your slot directory design if applicable.

It is composed of two parts: offset to the record(2 bytes) + record length(2 bytes)


### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.

I declared a vector of int inside the fileHandle class. Each time a fileHandle opens a file, it will scan all pages within the file, record the size of the 
free space for each page and push the size into the vector. When a record is inserted and the current page fails to hold the record, the fileHandle would check the vector to see if there is any suitable page for the current record. 

- How many hidden pages are utilized in your design?

Only one for metadata such as read/write/append counters.


- Show your hidden page(s) format design if applicable
It's an array with 3 elements of size 2. 


### 5. Implementation Detail
- Other implementation details goes here.

The design for #4 may not by time-efficient, since fileHandle may be used very frequently. But I think this way of implementation is more space-efficient. (eg. this implemenation reduces the unused space of each page to the minimum.)


### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
