#include "src/include/qe.h"

namespace PeterDB {
   


    Filter::Filter(Iterator *input, const Condition &condition) {
        filter_condition = condition; 
        filter_input = input;
        //current_tuple = (char* )malloc(PAGE_SIZE);
        input->getAttributes(filter_attributes); 

        if (condition.rhsValue.type == TypeInt || condition.rhsValue.type == TypeReal)
            right_value_int = *(int* )(condition.rhsValue.data); 
        else
        {
            int right_value_str_length = *(int* )(condition.rhsValue.data); 
            right_value_str.assign(SIZE_OF_INT + (char* )(condition.rhsValue.data), right_value_str_length); 
        }
    }

    Filter::~Filter() {
        //free(current_tuple);
    }

    RC Filter::getNextTuple(void *data) {
        
        int flag_terminate = 0;
        while (filter_input->getNextTuple(data) == 0)
        {
            
            int data_offset = 0; 
            int field_num = filter_attributes.size();
            char* bitmap_ptr = nullptr; 
            data_offset += ceil((double)field_num / (double)8); 

            for (int i = 0; i < field_num; i++)
            {
                int bit_offset = i % 8; 
                int byte_offset = i / 8; 

                bitmap_ptr = (char* )data + byte_offset; 
                char result = (*(bitmap_ptr) >> (7 - bit_offset)) & 1; 

                if (result == 1)
                    ;
                else if (result == 0)
                {                
                    Attribute curr_attribute = filter_attributes[i];
                    if (curr_attribute.type == TypeInt || curr_attribute.type == TypeReal)
                    {
                        if (curr_attribute.name == filter_condition.lhsAttr)
                        {
                            int left_value_int = *(int* )((char* )data + data_offset); 

                            if (filter_condition.op == CompOp::EQ_OP && left_value_int == right_value_int)
                                flag_terminate = 1; 
                            else if (filter_condition.op == CompOp::NE_OP && left_value_int != right_value_int)
                                flag_terminate = 1;
                            else if (filter_condition.op == CompOp::GE_OP && left_value_int >= right_value_int)
                                flag_terminate = 1; 
                            else if (filter_condition.op == CompOp::GT_OP && left_value_int > right_value_int)
                                flag_terminate = 1; 
                            else if (filter_condition.op == CompOp::LE_OP && left_value_int <= right_value_int)
                                flag_terminate = 1; 
                            else if (filter_condition.op == CompOp::LT_OP && left_value_int < right_value_int)
                                flag_terminate = 1; 
                        }
                        data_offset += SIZE_OF_INT; 
                    }     
                    else if (curr_attribute.type == TypeVarChar)
                    {
                        int left_value_str_length = *(int* )(data + data_offset); 
                        data_offset += SIZE_OF_INT; 
                        if (curr_attribute.name == filter_condition.lhsAttr)
                        {
                            std::string left_value_str; 
                            left_value_str.assign((char* )data + data_offset, left_value_str_length); 
                            
                            if (filter_condition.op == CompOp::EQ_OP && left_value_str == right_value_str)
                                flag_terminate = 1; 
                            else if (filter_condition.op == CompOp::NE_OP && left_value_str != right_value_str)
                                flag_terminate = 1;
                            else if (filter_condition.op == CompOp::GE_OP && left_value_str >= right_value_str)
                                flag_terminate = 1; 
                            else if (filter_condition.op == CompOp::GT_OP && left_value_str > right_value_str)
                                flag_terminate = 1; 
                            else if (filter_condition.op == CompOp::LE_OP && left_value_str <= right_value_str)
                                flag_terminate = 1; 
                            else if (filter_condition.op == CompOp::LT_OP && left_value_str < right_value_str)
                                flag_terminate = 1; 
                        }
                        data_offset += left_value_str_length; 
                    }
                }
                if (flag_terminate == 1)
                    break;
            }
            if (flag_terminate == 1)
                break;
        }
        if (flag_terminate == 1)
            return 0;
        else 
            return -1;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = filter_attributes;
        return 0;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        project_input = input;

        //std::vector<Attribute> attributes_from_lower_layer; 
        input->getAttributes(attributes_previous);

        for (const std::string& s : attrNames)
        {
            for (const Attribute& attr : attributes_previous)
            {
                if (s == attr.name)
                {
                    attributes_current.push_back(attr); 
                }
            }
        }

        current_tuple = (char* )malloc(PAGE_SIZE); 
    }

    Project::~Project() {
        free(current_tuple);
    }

    RC Project::getNextTuple(void *data) {
        if (project_input->getNextTuple(current_tuple) == 0)
        {
            int data_offset_current = 0; 
            
            int field_num_current = attributes_current.size();
            int field_num_previous = attributes_previous.size();
            
            char* bitmap_ptr_current = nullptr; 
            data_offset_current += ceil((double)field_num_current / (double)8); 

            for (int i = 0; i < field_num_current; i++)
            {
                //preparations
                int bit_offset_current = i % 8; 
                int byte_offset_current = i / 8; 
                bitmap_ptr_current = (char* )data + byte_offset_current; 

                if (i % 8 == 0)
                    *bitmap_ptr_current = 0;

                //for-loop
                int data_offset_previous = 0; 
                data_offset_previous += ceil((double)field_num_previous / (double)8); 
                char* bitmap_ptr_previous = nullptr; 

                for (int j = 0; j < field_num_previous; j++)
                {
                    int bit_offset_previous = i % 8; 
                    int byte_offset_previous = i / 8; 
                    bitmap_ptr_previous = (char* )current_tuple + byte_offset_previous; 

                    char result_previous = (*(bitmap_ptr_previous) >> (7 - bit_offset_previous)) & 1; 
                    
                    if (attributes_current[i].name == attributes_previous[j].name)
                    {
                        if (result_previous == 1)
                            *bitmap_ptr_current |= (1 << (7 - bit_offset_current)); 
                        else if (result_previous == 0)
                        {          
                            *bitmap_ptr_current |= (0 << (7 - bit_offset_current)); 
                            Attribute curr_attribute = attributes_previous[i];
                            if (curr_attribute.type == TypeInt || curr_attribute.type == TypeReal)
                            {
                                memmove(data + data_offset_current, current_tuple + data_offset_previous, SIZE_OF_INT);
                                
                                data_offset_previous += SIZE_OF_INT; 
                                data_offset_current += SIZE_OF_INT;
                            }
                            else if (curr_attribute.type == TypeVarChar)
                            {
                                int varchar_length = *(int* )(current_tuple + data_offset_previous); 
                                memmove(data + data_offset_current, current_tuple + data_offset_previous, SIZE_OF_INT + varchar_length);
                                
                                data_offset_previous += SIZE_OF_INT; 
                                data_offset_previous += varchar_length; 

                                data_offset_current += SIZE_OF_INT;
                                data_offset_current += varchar_length;
                            }
                        }
                        break;
                    }
                    else
                    {
                        if (result_previous == 1)
                            ;
                        else if (result_previous == 0)
                        {                
                            Attribute curr_attribute = attributes_previous[i];
                            if (curr_attribute.type == TypeInt || curr_attribute.type == TypeReal)
                                data_offset_previous += SIZE_OF_INT; 
                            else if (curr_attribute.type == TypeVarChar)
                            {
                                int varchar_length = *(int* )(current_tuple + data_offset_previous); 
                                data_offset_previous += SIZE_OF_INT; 
                                data_offset_previous += varchar_length; 
                            }
                        }
                    }
                }
            }
            return 0;
        }
        else
            return -1;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = attributes_current;
        return 0;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) 
        //:scanner(RelationManager::instance(), "Tmp_" + condition.lhsAttr + "_" + condition.rhsAttr)
    {
        join_left_it = leftIn; 
        join_right_it = rightIn; 

        //get attributes from two sides
        join_left_it->getAttributes(join_left_attrs);
        join_right_it->getAttributes(join_right_attrs);

        for (const Attribute& a : join_left_attrs)
        {
            join_all_attrs.push_back(a);
            join_all_attrs_name.push_back(a.name); 
        }

        for (const Attribute& a : join_right_attrs)
        {
            join_all_attrs.push_back(a); 
            join_all_attrs_name.push_back(a.name);
        }

        join_condition = condition; 
        join_numPages_allowed = numPages;
        
        output_page = (char* )malloc(PAGE_SIZE);
        temp_table_name = "Tmp_" + join_condition.lhsAttr + "_" + join_condition.rhsAttr;
        RelationManager::instance().createTable(temp_table_name, join_all_attrs); 
        flag_join_done = 0;

        //scanner = TableScan(RelationManager::instance(), "Tmp_" + condition.lhsAttr + "_" + condition.rhsAttr)
    }

    BNLJoin::~BNLJoin() {
        flag_join_done = 0;
        RelationManager::instance().deleteTable(temp_table_name); 
        free(output_page);
    }

    void find_key_in_tuple_and_break(void* data, const std::string& target_attribute_name, const std::vector<Attribute>& attributes, int& hashtable_key_int, std::string& hashtable_key_str, int& flag_attr_type_is_int)
    {
        int data_offset = 0; 
        int field_num = attributes.size();
        char* bitmap_ptr = nullptr; 
        data_offset += ceil((double)field_num / (double)8); 

        for (int i = 0; i < field_num; i++)
        {
            int bit_offset = i % 8; 
            int byte_offset = i / 8; 

            bitmap_ptr = (char* )data + byte_offset; 
            char result = (*(bitmap_ptr) >> (7 - bit_offset)) & 1; 

            if (result == 1)
                ;
            else if (result == 0)
            {                
                Attribute curr_attribute = attributes[i];
                if (curr_attribute.type == TypeInt || curr_attribute.type == TypeReal)
                {
                    if (curr_attribute.name == target_attribute_name)
                    {
                        hashtable_key_int = *(int* )((char* )data + data_offset); 
                        flag_attr_type_is_int = 1;
                        break;
                    }  
                    data_offset += SIZE_OF_INT; 
                }     
                else if (curr_attribute.type == TypeVarChar)
                {
                    int left_value_str_length = *(int* )(data + data_offset); 
                    data_offset += SIZE_OF_INT; 
                    if (curr_attribute.name == target_attribute_name)
                    {
                        hashtable_key_str.assign((char* )data + data_offset, left_value_str_length); 
                        flag_attr_type_is_int = 0;
                        break;
                    } 
                    data_offset += left_value_str_length; 
                }
            }    
        }
    }

    void find_key_in_tuple(void* data, const std::string& target_attribute_name, const std::vector<Attribute>& attributes, int& hashtable_key_int, std::string& hashtable_key_str, int& flag_attr_type_is_int, int& tuple_length, std::string& hashtable_value)
    {
        int data_offset = 0; 
        int field_num = attributes.size();
        char* bitmap_ptr = nullptr; 
        data_offset += ceil((double)field_num / (double)8); 

        for (int i = 0; i < field_num; i++)
        {
            int bit_offset = i % 8; 
            int byte_offset = i / 8; 

            bitmap_ptr = (char* )data + byte_offset; 
            char result = (*(bitmap_ptr) >> (7 - bit_offset)) & 1; 

            if (result == 1)
                ;
            else if (result == 0)
            {                
                Attribute curr_attribute = attributes[i];
                if (curr_attribute.type == TypeInt || curr_attribute.type == TypeReal)
                {
                    if (curr_attribute.name == target_attribute_name)
                    {
                        hashtable_key_int = *(int* )((char* )data + data_offset); 
                        flag_attr_type_is_int = 1;
                    }  
                    data_offset += SIZE_OF_INT; 
                }     
                else if (curr_attribute.type == TypeVarChar)
                {
                    int left_value_str_length = *(int* )(data + data_offset); 
                    data_offset += SIZE_OF_INT; 
                    if (curr_attribute.name == target_attribute_name)
                    {
                        hashtable_key_str.assign((char* )data + data_offset, left_value_str_length); 
                        flag_attr_type_is_int = 0;
                    } 
                    data_offset += left_value_str_length; 
                }
            }    
        }
        tuple_length = data_offset;
        hashtable_value.assign((char* )data, data_offset); 
    }

    void find_length_of_tuple(void* data, const std::vector<Attribute>& attributes, int& length)
    {
        int data_offset = 0; 
        int field_num = attributes.size();
        char* bitmap_ptr = nullptr; 
        data_offset += ceil((double)field_num / (double)8); 

        for (int i = 0; i < field_num; i++)
        {
            int bit_offset = i % 8; 
            int byte_offset = i / 8; 

            bitmap_ptr = (char* )data + byte_offset; 
            char result = (*(bitmap_ptr) >> (7 - bit_offset)) & 1; 

            if (result == 1)
                ;
            else if (result == 0)
            {                
                Attribute curr_attribute = attributes[i];
                if (curr_attribute.type == TypeInt || curr_attribute.type == TypeReal)
                    data_offset += SIZE_OF_INT; 
                else if (curr_attribute.type == TypeVarChar)
                {
                    int left_value_str_length = *(int* )(data + data_offset); 
                    data_offset += SIZE_OF_INT; 
                    data_offset += left_value_str_length; 
                }
            }    
        }
        length = data_offset;
    }

    int INLJoin::merge_tuples_and_write(const char* left_tuple, const char* right_tuple)
    {
        int field_num_left = join_left_attrs.size();
        int field_num_right = join_right_attrs.size();
        int field_num_all = join_all_attrs.size();

        int data_offset_left = ceil((double)field_num_left / (double)8); 
        int data_offset_right = ceil((double)field_num_right / (double)8); 
        int data_offset_all = ceil((double)field_num_all / (double)8); 

        char* bitmap_ptr_all = nullptr;

        //directly copy the null-indicator of the first tuple
        for (int i = 0; i < data_offset_left; i++)
        {
            *(output_page + i) = *(left_tuple + i);
        }

        for (int i = field_num_left; i < field_num_all; i++)
        {
            int bit_offset_all = i % 8; 
            int byte_offset_all = i / 8; 
            
            if (bit_offset_all == 0)
                *(output_page + byte_offset_all) = 0;

            int bit_offset_right = (i - field_num_left) % 8;
            int byte_offset_right = (i - field_num_left) / 8; 
            
            char result = (*(right_tuple + byte_offset_right) >> (7 - bit_offset_right)) & 1;
            
            bitmap_ptr_all = output_page + byte_offset_all; 
            
            if (result == 1)
            {
                *bitmap_ptr_all |= (1 << (7 - bit_offset_all)); 
            }
            else if (result == 0)
            {
                *bitmap_ptr_all |= (0 << (7 - bit_offset_all)); 
            }  
        }

        int left_tuple_length;
        int right_tuple_length;
        
        find_length_of_tuple((char* )left_tuple, join_left_attrs, left_tuple_length);
        find_length_of_tuple((char* )right_tuple, join_right_attrs, right_tuple_length);
        
        memmove(output_page + data_offset_all, left_tuple + data_offset_left, left_tuple_length - data_offset_left);
        data_offset_all += (left_tuple_length - data_offset_left); 
        memmove(output_page + data_offset_all, right_tuple + data_offset_right, right_tuple_length - data_offset_right);
        
        RID rid;
        return RelationManager::instance().insertTuple(temp_table_name, output_page, rid);
    }

    int merge_tuples_and_write(const std::string& left_tuple, const std::string& right_tuple, std::vector<Attribute> join_left_attrs, std::vector<Attribute> join_right_attrs, std::vector<Attribute> join_all_attrs, char* output_page, std::string table_name_to_insert)
    {
        
        int field_num_left = join_left_attrs.size();
        int field_num_right = join_right_attrs.size();
        int field_num_all = join_all_attrs.size();

        int data_offset_left = ceil((double)field_num_left / (double)8); 
        int data_offset_right = ceil((double)field_num_right / (double)8); 
        int data_offset_all = ceil((double)field_num_all / (double)8); 

        //char* bitmap_ptr_left = nullptr;
        //char* bitmap_ptr_right = nullptr;
        char* bitmap_ptr_all = nullptr;

        //directly copy the null-indicator of the first tuple
        for (int i = 0; i < data_offset_left; i++)
        {
            *(output_page + i) = left_tuple[i];
        }

        for (int i = field_num_left; i < field_num_all; i++)
        {
            int bit_offset_all = i % 8; 
            int byte_offset_all = i / 8; 
            
            if (bit_offset_all == 0)
                *(output_page + byte_offset_all) = 0;

            int bit_offset_right = (i - field_num_left) % 8;
            int byte_offset_right = (i - field_num_left) / 8; 
            
            char result = (right_tuple[byte_offset_right] >> (7 - bit_offset_right)) & 1; 
            bitmap_ptr_all = output_page + byte_offset_all; 
            
            if (result == 1)
            {
                *bitmap_ptr_all |= (1 << (7 - bit_offset_all)); 
            }
            else if (result == 0)
            {
                *bitmap_ptr_all |= (0 << (7 - bit_offset_all)); 
            }  
        }

        memmove(output_page + data_offset_all, left_tuple.c_str() + data_offset_left, left_tuple.size() - data_offset_left);
        data_offset_all += (left_tuple.size() - data_offset_left); 
        memmove(output_page + data_offset_all, right_tuple.c_str() + data_offset_right, right_tuple.size() - data_offset_right);
        
        RID rid;
        return RelationManager::instance().insertTuple(table_name_to_insert, output_page, rid);
    }

    RC BNLJoin::getNextTuple(void *data) {
        while (!flag_join_done)
        {
            int flag_full = 0;

            //We don't differentiate Int and Real. 
            //We use the default hash function.
            std::unordered_map<int, std::vector<std::string>> hashtable_int;
            std::unordered_map<std::string, std::vector<std::string>> hashtable_str;
            unsigned int bytes_loaded = 0;

            while (join_left_it->getNextTuple(data) == 0)
            {
                /*
                int data_offset = 0; 
                int field_num = join_left_attrs.size();
                char* bitmap_ptr = nullptr; 
                data_offset += ceil((double)field_num / (double)8); 

                int hashtable_key_int;
                std::string hashtable_key_str;

                int flag_type_is_int = -1;

                for (int i = 0; i < field_num; i++)
                {
                    int bit_offset = i % 8; 
                    int byte_offset = i / 8; 

                    bitmap_ptr = (char* )data + byte_offset; 
                    char result = (*(bitmap_ptr) >> (7 - bit_offset)) & 1; 

                    if (result == 1)
                        ;
                    else if (result == 0)
                    {                
                        Attribute curr_attribute = join_left_attrs[i];
                        if (curr_attribute.type == TypeInt || curr_attribute.type == TypeReal)
                        {
                            if (curr_attribute.name == join_condition.lhsAttr)
                            {
                                hashtable_key_int = *(int* )((char* )data + data_offset); 
                                flag_type_is_int = 1;
                            }  
                            data_offset += SIZE_OF_INT; 
                        }     
                        else if (curr_attribute.type == TypeVarChar)
                        {
                            int left_value_str_length = *(int* )(data + data_offset); 
                            data_offset += SIZE_OF_INT; 
                            if (curr_attribute.name == join_condition.lhsAttr)
                            {
                                hashtable_key_str.assign((char* )data + data_offset, left_value_str_length); 
                                flag_type_is_int = 0;
                            } 
                            data_offset += left_value_str_length; 
                        }
                    }    
                }
                std::string hashtable_value;
                hashtable_value.assign((char* )data, data_offset); 
                */
                int hashtable_key_int = 0; 
                std::string hashtable_key_str;
                //int:1; str:0
                int flag_key_type; 
                int tuple_length = 0;
                std::string hashtable_value;

                find_key_in_tuple(data, join_condition.lhsAttr, join_left_attrs, hashtable_key_int, hashtable_key_str, flag_key_type, tuple_length, hashtable_value);
                if (flag_key_type == 1)
                    hashtable_int[hashtable_key_int].push_back(hashtable_value);
                else 
                    hashtable_str[hashtable_key_str].push_back(hashtable_value);
                bytes_loaded += tuple_length;

                //Using less pages is to offset the overhead of strings
                if (bytes_loaded > (join_numPages_allowed - 1) * PAGE_SIZE)
                {
                    flag_full = 1;
                    break;
                }
            }

            while (join_right_it->getNextTuple(data) == 0)
            {
                int hashtable_key_int = 0; 
                std::string hashtable_key_str;
                //int:1; str:0
                int flag_key_type; 
                int tuple_length = 0;
                std::string hashtable_value;

                find_key_in_tuple(data, join_condition.rhsAttr, join_right_attrs, hashtable_key_int, hashtable_key_str, flag_key_type, tuple_length, hashtable_value);
                if (flag_key_type == 1)
                {
                    std::unordered_map<int, std::vector<std::string>>::const_iterator hashtable_values = hashtable_int.find(hashtable_key_int); 
                    if (hashtable_values == hashtable_int.end())
                        ;
                    else
                    {
                        for (const std::string& left_tuple : hashtable_values->second)
                        {
                            if (merge_tuples_and_write(left_tuple, hashtable_value, join_left_attrs, join_right_attrs, join_all_attrs, output_page, temp_table_name) == -1)
                                return -1;
                        }
                    }
                }
                else 
                {
                    std::unordered_map<std::string, std::vector<std::string>>::const_iterator hashtable_values = hashtable_str.find(hashtable_key_str); 
                    if (hashtable_values == hashtable_str.end())
                        ;
                    else
                    {
                        for (const std::string& left_tuple : hashtable_values->second)
                        {
                            if (merge_tuples_and_write(left_tuple, hashtable_value, join_left_attrs, join_right_attrs, join_all_attrs, output_page, temp_table_name) == -1)
                                return -1;
                        }
                    }
                }
            }
            if (flag_full == 0)
            {
                RelationManager::instance().scan(temp_table_name, "", NO_OP, nullptr, join_all_attrs_name, temp_table_iter);
                flag_join_done = 1;
                break;
            }
        }
        
        RID rid;
        return temp_table_iter.getNextTuple(rid, data);
        //RelationManager::instance().scan(temp_table_name, "", NO_OP, nullptr, join_all_attrs, RM_ScanIterator);
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = join_all_attrs;
        return 0;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        join_left_it = leftIn; 
        join_right_it = rightIn; 

        //get attributes from two sides
        join_left_it->getAttributes(join_left_attrs);
        join_right_it->getAttributes(join_right_attrs);

        for (const Attribute& a : join_left_attrs)
        {
            join_all_attrs.push_back(a);
            join_all_attrs_name.push_back(a.name); 
        }

        for (const Attribute& a : join_right_attrs)
        {
            join_all_attrs.push_back(a); 
            join_all_attrs_name.push_back(a.name);
        }

        join_condition = condition; 
        output_page = (char* )malloc(PAGE_SIZE);
        temp_table_name = "Tmp_" + join_condition.lhsAttr + "_" + join_condition.rhsAttr;
        RelationManager::instance().createTable(temp_table_name, join_all_attrs); 
        flag_join_done = 0;
        
        for (int i = 0; i < condition.rhsAttr.size(); i++)
        {
            if (condition.rhsAttr[i] == '.')
            {
                right_table_name.assign(condition.rhsAttr, 0, i);
                right_attr_name.assign(condition.rhsAttr, i + 1, std::string::npos);
                break;
            }
        }

        std::cout<<right_table_name<<std::endl;
        std::cout<<right_attr_name<<std::endl;

        
    }

    INLJoin::~INLJoin() {
        flag_join_done = 0;
        RelationManager::instance().deleteTable(temp_table_name); 
        free(output_page);
    }

    RC INLJoin::getNextTuple(void *data) {
        while (flag_join_done == 0 && join_left_it->getNextTuple(data) == 0)
        {
            int key_int; 
            std::string key_str;
            int key_type;

            find_key_in_tuple_and_break(data, join_condition.lhsAttr, join_left_attrs, key_int, key_str, key_type);
            if (key_type)
                join_right_it->setIterator(&key_int, &key_int, true, true);
            else
            {
                void* value = malloc(PAGE_SIZE);
                *(int* )value = key_str.size();
                memmove(value + SIZE_OF_INT, key_str.c_str(), key_str.size());
                join_right_it->setIterator(value, value, true, true);
                free(value);
            }
            
            char* right_data = (char* )malloc(PAGE_SIZE); 
            while(join_right_it->getNextTuple(right_data) == 0)
            {
                if (merge_tuples_and_write((char* )data, right_data) == -1)
                    return -1;
            }
            free(right_data);
        }
        if (flag_join_done == 0)
        {
            RelationManager::instance().scan(temp_table_name, "", NO_OP, nullptr, join_all_attrs_name, temp_table_iter);
            flag_join_done = 1;
        }
        RID rid;
        return temp_table_iter.getNextTuple(rid, data);
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = join_all_attrs;
        return 0;
    }

    //The hash function is derived from here: https://stackoverflow.com/questions/7666509/hash-function-for-string . Its name is "djb2", designed by Dan Bernstein.
    unsigned long hash_func_str(std::string hashtable_key_str)
    {
        unsigned long hash = 5381;
        int c;
        for (const char& i : hashtable_key_str)
        {
            c = i;
            hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
        }
        return hash;
    }
 
    //https://stackoverflow.com/questions/6082915/a-good-hash-function-to-use-in-interviews-for-integer-numbers-strings
    unsigned long hash_func_int(int hashtable_key_int)
    {   
        unsigned long ret = 17;
        return ret * 31 + hashtable_key_int;
    }

    void build_disk_hashtables(const std::string& attr_name, const std::vector<Attribute> attrs, int& tuple_num, Iterator* it, const int& partition_num)
    {
        tuple_num = 0;
        void* data = malloc(PAGE_SIZE);
        while (it->getNextTuple(data) == 0)
        {
            int hashtable_key_int = 0; 
            std::string hashtable_key_str;
            int flag_key_type; 
            find_key_in_tuple_and_break(data, attr_name, attrs, hashtable_key_int, hashtable_key_str, flag_key_type); 
            
            unsigned long hash_value = 0;
            if (flag_key_type == 1)
                hash_value = hash_func_int(hashtable_key_int);
            else 
                hash_value = hash_func_str(hashtable_key_str);
            
            int bucket_index = hash_value % partition_num;
            RID rid;
            std::string bucket_name = "Partition" + attr_name + std::to_string(bucket_index);
            RelationManager::instance().insertTuple(bucket_name, data, rid);
            tuple_num++;
        }
        free(data); 
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {
        join_left_it = leftIn;
        join_right_it = rightIn;
        join_condition = condition;
        partitions_number = numPartitions;
        flag_join_done = 0;

        //get attributes from two sides
        join_left_it->getAttributes(join_left_attrs);
        join_right_it->getAttributes(join_right_attrs);

        for (const Attribute& a : join_left_attrs)
        {
            join_all_attrs.push_back(a);
            join_all_attrs_name.push_back(a.name); 
            join_left_attrs_name.push_back(a.name);
        }

        for (const Attribute& a : join_right_attrs)
        {
            join_all_attrs.push_back(a); 
            join_all_attrs_name.push_back(a.name);
            join_right_attrs_name.push_back(a.name);
        }

        output_page = (char* )malloc(PAGE_SIZE);
        output_table_name = "Tmp_" + join_condition.lhsAttr + "_" + join_condition.rhsAttr;
        RelationManager::instance().createTable(output_table_name, join_all_attrs); 
        //flag_join_done = 0;

        //initiate hashtables
        for (int i = 0; i < partitions_number; i++)
        {
            RelationManager::instance().createTable("Partition" + join_condition.lhsAttr + std::to_string(i), join_left_attrs); 
            RelationManager::instance().createTable("Partition" + join_condition.rhsAttr + std::to_string(i), join_right_attrs);
        }        
    }

    GHJoin::~GHJoin() {
        for (int i = 0; i < partitions_number; i++)
        {
            RelationManager::instance().deleteTable("Partition" + join_condition.lhsAttr + std::to_string(i)); 
            RelationManager::instance().deleteTable("Partition" + join_condition.rhsAttr + std::to_string(i));
        }
        RelationManager::instance().deleteTable(output_table_name);
        free(output_page);
    }

    RC GHJoin::getNextTuple(void *data) {
        if (!flag_join_done)
        {
                        
            //build hashtables
            int left_tuple_num;
            build_disk_hashtables(join_condition.lhsAttr, join_left_attrs, left_tuple_num, join_left_it, partitions_number);
            int right_tuple_num;
            build_disk_hashtables(join_condition.rhsAttr, join_right_attrs, right_tuple_num, join_right_it, partitions_number);

            //current_partition = 0;

            for (int current_partition = 0; current_partition < partitions_number; current_partition++)
            {
                std::string left_bucket_name = "Partition" + join_condition.lhsAttr + std::to_string(current_partition); 
                std::string right_bucket_name = "Partition" + join_condition.rhsAttr + std::to_string(current_partition); 

                //TableScan left_scanner(RelationManager::instance(), left_bucket_name); 
                //TableScan right_scanner(RelationManager::instance(), right_bucket_name);

                RM_ScanIterator left_scanner;
                RM_ScanIterator right_scanner;

                RelationManager::instance().scan(left_bucket_name, "", NO_OP, NULL, join_left_attrs_name, left_scanner);
                RelationManager::instance().scan(right_bucket_name, "", NO_OP, NULL, join_right_attrs_name, right_scanner);
                
                //preparations
                char* temp_data = (char* )malloc(PAGE_SIZE);
                std::unordered_map<int, std::vector<std::string>> hashtable_int;
                std::unordered_map<std::string, std::vector<std::string>> hashtable_str;

                RID rid;
                int i = 0;
                while (left_scanner.getNextTuple(rid, temp_data) == 0)
                {
                    i++;
                    int hashtable_key_int = 0; 
                    std::string hashtable_key_str;
                    //int:1; str:0
                    int flag_key_type; 
                    int tuple_length = 0;
                    std::string hashtable_value;

                    find_key_in_tuple(temp_data, join_condition.lhsAttr, join_left_attrs, hashtable_key_int, hashtable_key_str, flag_key_type, tuple_length, hashtable_value);
                    if (flag_key_type == 1)
                        hashtable_int[hashtable_key_int].push_back(hashtable_value);
                    else 
                        hashtable_str[hashtable_key_str].push_back(hashtable_value);
                }
                //std::cout<<"There are "<<i<<"keys in the left Bucket "<<current_partition<<std::endl;
                int j = 0;
                while (right_scanner.getNextTuple(rid, temp_data) == 0)
                {
                    j++;
                    int hashtable_key_int = 0; 
                    std::string hashtable_key_str;
                    //int:1; str:0
                    int flag_key_type; 
                    int tuple_length = 0;
                    std::string hashtable_value;

                    find_key_in_tuple(temp_data, join_condition.rhsAttr, join_right_attrs, hashtable_key_int, hashtable_key_str, flag_key_type, tuple_length, hashtable_value);
                    if (flag_key_type == 1)
                    {
                        std::unordered_map<int, std::vector<std::string>>::const_iterator hashtable_values = hashtable_int.find(hashtable_key_int); 
                        if (hashtable_values == hashtable_int.end())
                            ;
                        else
                        {
                            for (const std::string& left_tuple : hashtable_values->second)
                            {
                                if (merge_tuples_and_write(left_tuple, hashtable_value, join_left_attrs, join_right_attrs, join_all_attrs, output_page, output_table_name) == -1)
                                {
                                    std::cout<<"Merge Failed: int@GHJ"<<std::endl;
                                    std::cout<<"["<<current_partition<<","<<j<<"]"<<std::endl;
                                    return -1;
                                    //break;
                                }
                                
                            }
                        }
                    }
                    else 
                    {
                        std::unordered_map<std::string, std::vector<std::string>>::const_iterator hashtable_values = hashtable_str.find(hashtable_key_str); 
                        if (hashtable_values == hashtable_str.end())
                            ;
                        else
                        {
                            for (const std::string& left_tuple : hashtable_values->second)
                            {
                                if (merge_tuples_and_write(left_tuple, hashtable_value, join_left_attrs, join_right_attrs, join_all_attrs, output_page, output_table_name) == -1)
                                {
                                    std::cout<<"Merge Failed: str@GHJ"<<std::endl;
                                    std::cout<<"["<<current_partition<<","<<j<<"]"<<std::endl;
                                    return -1;
                                }
                            }
                        }
                    }
                    //std::cout<<"There are "<<j<<"keys in the right Bucket "<<current_partition<<std::endl;
                }
                free(temp_data);
            }
            RelationManager::instance().scan(output_table_name, "", NO_OP, NULL, join_all_attrs_name, output_iter);
            flag_join_done = 1;
        }
        RID rid;
        return output_iter.getNextTuple(rid, data); 
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = join_all_attrs;
        return 0;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
        aggregate_input = input;
        aggregate_project_attr = aggAttr; 
        aggregate_op = op;

        aggregate_max = 0; 
        aggregate_min = 0; 
        aggregate_count = 0;
        aggregate_sum = 0;

        flag_first_loop = 1;
        flag_returned = 0;
        flag_groupby = 0;

        input->getAttributes(aggregate_attrs);
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
        aggregate_input = input;
        aggregate_project_attr = aggAttr; 
        aggregate_group_attr = groupAttr;
        aggregate_op = op;

        aggregate_max = 0; 
        aggregate_min = 0; 
        aggregate_count = 0;
        aggregate_sum = 0;

        flag_first_loop = 1;
        flag_returned = 0;
        flag_groupby = 1;
        flag_hashtable_built = 0;

        input->getAttributes(aggregate_attrs);
    }

    Aggregate::~Aggregate() {

    }
    
   
    RC Aggregate::getNextTuple(void *data) {
        
        if (!flag_groupby)
        {
            if (flag_returned)
                return -1;

            while (aggregate_input->getNextTuple(data) == 0)
            {
                int key_int; 
                std::string key_str;
                int key_type;
                float key_real;

                find_key_in_tuple_and_break(data, aggregate_project_attr.name, aggregate_attrs, key_int, key_str, key_type);

                if (aggregate_project_attr.type == AttrType::TypeReal)
                    key_real = *(float* )(&key_int);
                else if (aggregate_project_attr.type == AttrType::TypeInt)
                    key_real = static_cast<float>(key_int);
                
                if (flag_first_loop == 1)
                {
                    aggregate_min = key_real;
                    aggregate_max = key_real;
                }   
                else if (flag_first_loop == 0)
                {
                    aggregate_min = std::min(key_real, aggregate_min);
                    aggregate_max = std::max(key_real, aggregate_max);
                }    
                
                if (flag_first_loop == 1)
                    flag_first_loop = 0;

                aggregate_count += 1; 
                aggregate_sum += key_real;
            }

            float result; 
            if (aggregate_op == AggregateOp::AVG)
                result = aggregate_sum / aggregate_count;
            else if (aggregate_op == AggregateOp::COUNT)
                result = aggregate_count; 
            else if (aggregate_op == AggregateOp::MAX)
                result = aggregate_max;
            else if (aggregate_op == AggregateOp::MIN)
                result = aggregate_min;
            else if (aggregate_op == AggregateOp::SUM)
                result = aggregate_sum;
            
            *(char* )data = 0;
            *(float* )(data + 1) = result;

            flag_returned = 1;
            return 0;
        }
        else if (flag_groupby)
        {
            while (!flag_hashtable_built&&aggregate_input->getNextTuple(data) == 0)
            {                
                int hashtable_key_int = 0; 
                std::string hashtable_key_str;
                
                //int:1; str:0
                int flag_key_type; 
                int tuple_length = 0;
                std::string hashtable_value;

                find_key_in_tuple(data, aggregate_group_attr.name, aggregate_attrs, hashtable_key_int, hashtable_key_str, flag_key_type, tuple_length, hashtable_value);
                
                int hashtable_val_int = 0; 
                std::string hashtable_val_str;
                int flag_val_type; 
                find_key_in_tuple_and_break(data, aggregate_project_attr.name, aggregate_attrs, hashtable_val_int, hashtable_val_str, flag_val_type);
                
                float hashtable_val_real;

                if (aggregate_project_attr.type == AttrType::TypeReal)
                    hashtable_val_real = *(float* )(&hashtable_val_int);
                else if (aggregate_project_attr.type == AttrType::TypeInt)
                    hashtable_val_real = static_cast<float>(hashtable_val_int);

                if (flag_key_type == 1)
                {
                    if (hashtable_int[hashtable_key_int].cnt == 0)
                    {
                        hashtable_int[hashtable_key_int].min = hashtable_val_real;
                        hashtable_int[hashtable_key_int].max = hashtable_val_real;
                    }
                    else
                    {
                        hashtable_int[hashtable_key_int].min = std::min(hashtable_int[hashtable_key_int].min, hashtable_val_real);
                        hashtable_int[hashtable_key_int].max = std::max(hashtable_int[hashtable_key_int].max, hashtable_val_real);
                    }
                    hashtable_int[hashtable_key_int].cnt += 1; 
                    hashtable_int[hashtable_key_int].sum += hashtable_val_real;
                }
                else 
                {
                    if (hashtable_str[hashtable_key_str].cnt == 0)
                    {
                        hashtable_str[hashtable_key_str].min = hashtable_val_real;
                        hashtable_str[hashtable_key_str].max = hashtable_val_real;
                    }
                    else
                    {
                        hashtable_str[hashtable_key_str].min = std::min(hashtable_str[hashtable_key_str].min, hashtable_val_real);
                        hashtable_str[hashtable_key_str].max = std::max(hashtable_str[hashtable_key_str].max, hashtable_val_real);
                    }
                    hashtable_str[hashtable_key_str].cnt += 1; 
                    hashtable_str[hashtable_key_str].sum += hashtable_val_real;
                }
            }
            if (flag_hashtable_built == 0)
            {
                flag_hashtable_built = 1;
                hashtable_int_iter = hashtable_int.begin();
                hashtable_str_iter = hashtable_str.begin();
            }

            
            if (aggregate_group_attr.type == AttrType::TypeReal || aggregate_group_attr.type == AttrType::TypeInt)
            {
                if (hashtable_int_iter == hashtable_int.end())
                    return -1;
                
                int hashtable_key_int = hashtable_int_iter->first;
                float result; 
                if (aggregate_op == AggregateOp::AVG)
                    result = hashtable_int_iter->second.sum / hashtable_int_iter->second.cnt;
                else if (aggregate_op == AggregateOp::COUNT)
                    result = hashtable_int_iter->second.cnt; 
                else if (aggregate_op == AggregateOp::MAX)
                    result = hashtable_int_iter->second.max;
                else if (aggregate_op == AggregateOp::MIN)
                    result = hashtable_int_iter->second.min;
                else if (aggregate_op == AggregateOp::SUM)
                    result = hashtable_int_iter->second.sum;
                
                int offset = 0;
                *(char* )data = 0;
                offset += 1;
                *(int* )(data + offset) = hashtable_key_int;
                offset += SIZE_OF_INT; 
                *(float* )(data + offset) = result;
                offset += SIZE_OF_INT;

                hashtable_int_iter++;
                return 0;
            }
            else if (aggregate_group_attr.type == AttrType::TypeVarChar)
            {
                if (hashtable_str_iter == hashtable_str.end())
                    return -1;
                
                std::string hashtable_key_str = hashtable_str_iter->first;
                float result; 
                if (aggregate_op == AggregateOp::AVG)
                    result = hashtable_str_iter->second.sum / hashtable_str_iter->second.cnt;
                else if (aggregate_op == AggregateOp::COUNT)
                    result = hashtable_str_iter->second.cnt; 
                else if (aggregate_op == AggregateOp::MAX)
                    result = hashtable_str_iter->second.max;
                else if (aggregate_op == AggregateOp::MIN)
                    result = hashtable_str_iter->second.min;
                else if (aggregate_op == AggregateOp::SUM)
                    result = hashtable_str_iter->second.sum;

                int offset = 0;
                *(char* )data = 0;
                offset += 1;
                int hashtable_key_str_length = hashtable_key_str.size();
                *(int* )(data + offset) = hashtable_key_str_length;
                offset += SIZE_OF_INT; 
                memmove(data + offset, hashtable_key_str.c_str(), hashtable_key_str_length);
                offset += hashtable_key_str_length; 
                *(float* )(data + offset) = result;
                offset += SIZE_OF_INT;

                hashtable_str_iter++;
                return 0;
            }
        }
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();

        if (flag_groupby)
        {
            attrs.push_back(aggregate_group_attr); 
        }    
        Attribute a;
        a.type = AttrType::TypeReal;
        a.length = SIZE_OF_INT;
        
        if (aggregate_op == AggregateOp::AVG)
            a.name = "AVG(" + aggregate_project_attr.name + ")";
        else if (aggregate_op == AggregateOp::COUNT)
            a.name = "COUNT(" + aggregate_project_attr.name + ")";
        else if (aggregate_op == AggregateOp::MAX)
            a.name = "MAX(" + aggregate_project_attr.name + ")";
        else if (aggregate_op == AggregateOp::MIN)
            a.name = "MIN(" + aggregate_project_attr.name + ")";
        else if (aggregate_op == AggregateOp::SUM)
            a.name = "SUM(" + aggregate_project_attr.name + ")";
        attrs.push_back(a); 
        return 0;
    }
} // namespace PeterDB