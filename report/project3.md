## Project 3 Report


### 1. Basic information
 - Team #:17
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-fall20-team-17
 - Student 1 UCI NetID:qingchy
 - Student 1 Name:Qingchuan Yang
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 

There is only one explicit meta-data page in the index file, and the page only contains an integer, which is the page number for the root. 
Besides this, there is a hidden page created by PFM in order to collect the read/write/append counts.


### 3. Index Entry Format
- Show your index entry design (structure). 

We keep a directory at the back of a page. Therefore all the entries within a page are contiguous, and the directory will help locate and read a record. 

  - entries on internal nodes:  

They are pure data(e.g. int, real and varchar(without length)), no delimiters.   

  - entries on leaf nodes:

Same as internal nodes.


### 4. Page Format
- Show your internal-page (non-leaf node) design.

The page is divided into four parts, and the order is: records(index entry), white space, slot descriptors and page descriptor. 
A slot descriptor is composed of the following parts: a RID, a pointer to its child page, length of the record, offset to the record.   
A page descriprot stores the meta-data of the page: type(leaf or non-leaf), next page(unused for non-leaf nodes), offset to the white space, number of slot descriptors.
Records are contiguous, with no delimiter in between. 

- Show your leaf-page (leaf node) design.

We use the same format for the leaf nodes as the non-leaf nodes.
The major difference is that:
1)For the leaf node, the pointer to the child page in the slot descriptors is unused.
2)For the non-leaf node, the pointer to the next page in the page descriptor is unused.
3)The offset and length of the last slot in a non-leaf node are unused. 


### 5. Describe the following operation logic.
- Split

when a page(whether leaf or non-leaf) is full, and there is a new record to be inserted. We first allcoate another page called "neighbor page".
We split the current page into 2 parts--some records(smaller than a value) stay, others goe to the neighbor page. (Notice splitting a non-leaf node requires more calculation.)
Then we insert the new record into one of the two pages.(It depends on the value of the new record.)
In the above procedures, we also collect the key and the rid to be pushed up or copied up. 

Finally we check if our node is a root node or not. 
-If so, we allocate a new node and insert the new key into the root node. 
-Else, we simply return. (The key and the rid should be read by a higher level.)

- Rotation (if applicable)



- Merge/non-lazy deletion (if applicable)



- Duplicate key span in a page

When inserting a key, not only do we compare the key-values but also the RIDs. Therefore, the keys spanning a page are ordered by first, key values, and then, RIDs, in the case their key values are the same.

- Duplicate key span multiple pages (if applicable)

Like I said, the non-leaf nodes follow the same format as the leaf nodes, and therefore, they can deal with the 'duplicate key' problem automatically. 

### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.

No.


- Other implementation details:

I didn't strictly follow the procedure in the book. But the implementation has been proved mostly correct by the tests.
I also implemented binary search (to insert) on the leaf node, but I didn't employ it in my last commit, as I didn't see much performace improvement. 
The binary search idea was borrowed from here: https://stackoverflow.com/questions/22123489/olog-n-algorithm-to-find-best-insert-position-in-sorted-array


### 7. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 8. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
