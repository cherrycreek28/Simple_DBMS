## Project 2 Report


### 1. Basic information
 - Team #:17
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-fall20-team-17
 - Student 1 UCI NetID:qingchy
 - Student 1 Name:Qingchuan Yang
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.
As I call createCatalog(), 
2 records will be inserted into the "Tables" file, indicating there are 2 tables,
and 8 records will be inserted into the "Columns" file, indicating there are 8 attributes(columns) in total for the table "Tables" and table "Columns."

Everytime I createTable().
1 record will be inserted into the "Tables" file, indicating we are adding 1 table,
and n records will be inserted into the "Columns" file, indicating the new table contains n attributes.

//copied from project2 guide:
For the record in "Tables," We keep a record_descriptor as follows: Tables (table-id:int, table-name:varchar(50), file-name:varchar(50)).
For the record in "Columns," We keep a record_descriptor as follows: Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int).

These two record_descriptors, as well as [the number of tables] are kept in the RecordManager class, and everytime I call the RM constructor, the record_descriptors will be loaded and [the number of tables] will be counted by reading the "Tables" file.

### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
I didn't change the record format so this part is exactly same as in project1.md.
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

### 4. Page Format (in case you have changed from P1, please re-enter here)
I didn't change the page format so this part is exactly same as in project1.md.
- Show your page format design.

page
= actual records + empty space + descriptor for each records(4 bytes for each record) + page descriptor(4 bytes)

- Explain your slot directory design if applicable.

It is composed of two parts: offset to the record(2 bytes) + record length(2 bytes)

### 5. Page Management (in case you have changed from P1, please re-enter here)
I didn't change the page format so this part is exactly same as in project1.md.
- Show your algorithm of finding next available-space page when inserting a record.

I declared a vector of int inside the fileHandle class. Each time a fileHandle opens a file, it will scan all pages within the file, record the size of the 
free space for each page and push the size into the vector. When a record is inserted and the current page fails to hold the record, the fileHandle would check the vector to see if there is any suitable page for the current record. 

- How many hidden pages are utilized in your design?

Only one for metadata such as read/write/append counters.

- Show your hidden page(s) format design if applicable

It's an array with 3 elements of size 2. 

### 6. Describe the following operation logic.
- Delete a record
RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

Given the rid, first we visit the record metadata. 
-We check if the "offset" field is -1, if so, this means the record has already been deleted. We return -1.
-Then we check if the "length" field is -1, if so, this means the record has been moved to another page. We read the new rid from the record and perform a recursive deleteRecord() based on the new rid.
-If neither of the two fields is -1, we will perform deletion on the current page. 
    We use memmove() to move [all the subsequent records of the record to be deleted] forward to cover [the record to be deleted].
    We loop through all the metadata of records and change the "offset" field of one metadata if we find its offset is greater than [the record deleted]. 

- Update a record
RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

Given the rid, first we visit the record metadata. 
-We check if the "offset" field is -1, if so, this means the record has already been deleted. We return -1.
-Then we check if the "length" field is -1, if so, this means the record has been moved to another page. We read the new rid from the record and perform a recursive updateRecord() based on the new rid.
-If neither of the two fields is -1, we will perform updation on the current page.
    We calculate the length of new record and see if it can fit in the current page.
    -If so, we simply move [all the subsequent records of the record to be updated] forward or backword, and change their offset.
    -If not: 
        We change the "length" field of metadata of the record to -1 to indicate it's been moved.
        We call insertRecord() to insert the updated record and read the new RID.
        We cast the [pointer to record] to [pointer to a 8-byte RID] and fill out it with the new RID information. 
        We move [all the subsequent records of the record to be updated] forward, fill in the hole of [length of the record deleted - 8 bytes], and change the offset.


- Scan on normal records
We maintain a RID inside the iterator.
Each time, we read the attribute value from the record of our rid, compare the value with the given [void* value] and decide if the attribute qualifies the [compOp] requirement.  
-If it qualifies, we retrieve all the fields and copy them into memeory. 
-If not, we increase [rid.slot_number] by 1 and move to the next record. If the rid.slot_number exceeds the max slot number on that page, we increase [rid.page_number] by 1.


- Scan on deleted records
We check if the "offset" field in the metadata of the record is -1, if so, we simply skip the record by increaseing [rid.slot_number] by 1. 


- Scan on updated records
We check if the "length" field in the metadata of the record is -1, if so, we simply skip the record by increaseing [rid.slot_number] by 1. 


### 7. Implementation Detail
- Other implementation details goes here.
I use a std::vector<std::set<int>> to keep track of the free slots on each page. 


### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)