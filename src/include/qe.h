#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>

#include "rm.h"
#include "ix.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;


            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        ~IndexScan() override {
            iter.close();
        };
    };

    class Filter : public Iterator {
        // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

    private:
        Iterator* filter_input;
        Condition filter_condition;
        std::vector<Attribute> filter_attributes;
        char* current_tuple;
        int right_value_int;
        std::string right_value_str; 
    };

    class Project : public Iterator {
        // Projection operator
    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    private:
        Iterator* project_input;
        std::vector<Attribute> attributes_current;
        std::vector<Attribute> attributes_previous;
        char* current_tuple;
    };

    class BNLJoin : public Iterator {
        // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
        private:
        Iterator* join_left_it;
        TableScan* join_right_it;
        std::vector<std::string> join_all_attrs_name;
        std::vector<Attribute> join_left_attrs;
        std::vector<Attribute> join_right_attrs;
        std::vector<Attribute> join_all_attrs;
        Condition join_condition; 
        unsigned join_numPages_allowed;
        char* output_page;
        //int merge_tuples_and_write(const std::string& left_tuple, const std::string& right_tuple);
        std::string temp_table_name;
        int flag_join_done;
        RM_ScanIterator temp_table_iter;
        //TableScan scanner;
    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
        private:
        Iterator* join_left_it;
        IndexScan* join_right_it;
        std::vector<std::string> join_all_attrs_name;
        std::vector<Attribute> join_left_attrs;
        std::vector<Attribute> join_right_attrs;
        std::vector<Attribute> join_all_attrs;
        Condition join_condition; 
        //unsigned join_numPages_allowed;
        std::string temp_table_name;
        char* output_page;
        int flag_join_done;
        RM_ScanIterator temp_table_iter;
        //RM_IndexScanIterator temp_index_iter;

        std::string right_table_name;
        std::string right_attr_name;

        int merge_tuples_and_write(const char* left_tuple, const char* right_tuple);
    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    private:
        Iterator* join_left_it;
        Iterator* join_right_it;

        std::vector<std::string> join_all_attrs_name;
        std::vector<std::string> join_left_attrs_name;
        std::vector<std::string> join_right_attrs_name;
        std::vector<Attribute> join_left_attrs;
        std::vector<Attribute> join_right_attrs;
        std::vector<Attribute> join_all_attrs;
        
        Condition join_condition; 
        unsigned partitions_number;
        char* output_page;
        //int merge_tuples_and_write(const std::string& left_tuple, const std::string& right_tuple);
        std::string output_table_name;
        int flag_join_done;
        RM_ScanIterator output_iter;
        //unsigned current_partition;
        BNLJoin* current_bnj;

        //TableScan* current_left_scanner;
        //TableScan* current_right_scanner;
    };

    typedef struct aggregate_values
    {
        float max = 0; 
        float min = 0; 
        float sum = 0;
        //float avg;
        float cnt = 0;
        //bool initiated;
    }aggregate_values;

    class Aggregate : public Iterator {
        // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    private:
        Iterator* aggregate_input;
        Attribute aggregate_project_attr;
        AggregateOp aggregate_op;
        Attribute aggregate_group_attr;
        std::vector<Attribute> aggregate_attrs;

        int flag_first_loop;
        int flag_returned;
        int flag_groupby;
        int flag_hashtable_built;

        std::unordered_map<int, aggregate_values> hashtable_int;
        std::unordered_map<std::string, aggregate_values> hashtable_str;

        std::unordered_map<int, aggregate_values>::const_iterator hashtable_int_iter;
        std::unordered_map<std::string, aggregate_values>::const_iterator hashtable_str_iter;

        float aggregate_max; 
        float aggregate_min; 
        float aggregate_count;
        float aggregate_sum;
    };
} // namespace PeterDB

#endif // _qe_h_
