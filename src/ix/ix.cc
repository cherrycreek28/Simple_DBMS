#include "src/include/ix.h"
#include <memory>

//struct leaf_slot; 

namespace PeterDB {
    
    //return the page number of the root page
    int IXFileHandle::read_root_page_num()
    {
        char* root_ptr_page = (char* )malloc(PAGE_SIZE); 
        if (file_handle.readPage(0, root_ptr_page) == -1)
        {
            free(root_ptr_page); 
            return -1;
        }
        int root_ptr = *(int* )root_ptr_page;
        free(root_ptr_page);
        return root_ptr;
    }
    int IXFileHandle::write_root_page_num(int new_root_page_num)
    {
        char* root_ptr_page = (char* )malloc(PAGE_SIZE); 
        if (file_handle.readPage(0, root_ptr_page) == -1)
        {
            free(root_ptr_page); 
            return -1;
        }
        *(int* )root_ptr_page = new_root_page_num;
        if (file_handle.writePage(0, root_ptr_page) == -1)
        {
            free(root_ptr_page); 
            return -1;
        }
        return 0;
    }
    typedef struct slot_descriptor
    {
        short offset; 
        short length; 
        short pointer;
        RID rid;
    } slot_descriptor;
    typedef struct page_descriptor
    {
        short offset; 
        short next_page; 
        short slot_num; 
        short if_leaf;
    } page_descriptor;

    constexpr int SIZE_OF_SLOT_DESCRIPTOR = sizeof(slot_descriptor);
    constexpr int SIZE_OF_PAGE_DESCRIPTOR = sizeof(page_descriptor); 
    int rid_greater(const RID& rid1, const RID& rid2);

    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    int binary_search(void* current_page, int slot_num_end, int slot_num_begin, const int& key_to_insert_int, std::string& key_to_insert_str, const RID& rid_to_insert, Attribute attribute)
    {
        int slot_num_mid = -1; 
        while (slot_num_begin < slot_num_end)
        {
            slot_num_mid = (slot_num_begin + slot_num_end)/2; 
            slot_descriptor* middle_slot = (slot_descriptor* )(current_page + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR - SIZE_OF_SLOT_DESCRIPTOR * slot_num_mid);
            
            if (attribute.type == TypeInt || attribute.type == TypeReal)
            {
                int val_to_compare_int = *(int* )(current_page + middle_slot->offset); 
                if (val_to_compare_int > key_to_insert_int || (val_to_compare_int == key_to_insert_int && rid_greater(middle_slot->rid, rid_to_insert)))
                    slot_num_end = slot_num_mid; 
                else
                    slot_num_begin = slot_num_mid + 1;
            }
            else if (attribute.type == TypeVarChar)
            {
                std::string val_to_compare_str;
                val_to_compare_str.assign((char* )current_page + middle_slot->offset, middle_slot->length);
                if (val_to_compare_str > key_to_insert_str || (val_to_compare_str == key_to_insert_str && rid_greater(middle_slot->rid, rid_to_insert)))
                    slot_num_end = slot_num_mid; 
                else
                    slot_num_begin = slot_num_mid + 1;
            }
        }
        return slot_num_begin;
    }

    //This function determines if a new key can fit into the current page. 
    bool if_new_entry_fit(page_descriptor* page_des_leaf, int key_size)
    {
        return !(page_des_leaf->offset + page_des_leaf->slot_num * SIZE_OF_SLOT_DESCRIPTOR + SIZE_OF_PAGE_DESCRIPTOR + SIZE_OF_SLOT_DESCRIPTOR + key_size > PAGE_SIZE); 
    }

    void split_push_up(char* old_node, char* new_node, int& key_to_push_up_int, std::string& key_to_push_up_str, RID& rid_to_push_up, const Attribute& attribute)
    {
        page_descriptor* page_des_old = (page_descriptor* )(old_node + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 
        page_descriptor* page_des_new = (page_descriptor* )(new_node + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 

        page_des_new->next_page = -1; 
        page_des_new->offset = 0; 
        page_des_new->slot_num = 0; 
        page_des_new->if_leaf = 0;

        int slot_num_first = page_des_old->slot_num / 2 + 1; 
        int slot_num_second = page_des_old->slot_num - slot_num_first;
        slot_descriptor* new_page_begin_slot =  (slot_descriptor*)(old_node + PAGE_SIZE - (slot_num_first + 1) * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
        slot_descriptor* old_page_end_slot = (slot_descriptor*)(old_node + PAGE_SIZE - slot_num_first * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);

        //slot
        void* src_back = old_node + PAGE_SIZE - page_des_old->slot_num * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR; 
        int length_back = slot_num_second * SIZE_OF_SLOT_DESCRIPTOR;
        void* des_back = new_node + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR - length_back; 
        memmove(des_back, src_back, length_back); 

        //deal with data
        void* src_front = old_node + new_page_begin_slot->offset;
        int length_front = page_des_old->offset - new_page_begin_slot->offset;
        memmove(new_node, src_front, length_front);

        //regular case
        page_des_old->slot_num = slot_num_first; 
        page_des_old->offset -= length_front;
        page_des_old->offset -= old_page_end_slot->length;

        page_des_new->slot_num = slot_num_second;
        page_des_new->offset = length_front;

        for (int i = 1; i <= page_des_new->slot_num; i++)
        {
            slot_descriptor* current_slot = (slot_descriptor* )(new_node + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR - i * SIZE_OF_SLOT_DESCRIPTOR); 
            current_slot->offset -= new_page_begin_slot->offset;
        }
        
        rid_to_push_up = old_page_end_slot->rid;

        if (attribute.type == TypeInt || attribute.type == TypeReal)
            key_to_push_up_int = *(int* )(old_node + old_page_end_slot->offset);
        else
            key_to_push_up_str.assign(old_node + old_page_end_slot->offset, old_page_end_slot->length);
    }
    
    void split_copy_up(char* old_node, char* new_node, int page_num_new)
    {
        page_descriptor* page_des_old = (page_descriptor* )(old_node + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 
        page_descriptor* page_des_new = (page_descriptor* )(new_node + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 

        page_des_new->next_page = -1; 
        page_des_new->offset = 0; 
        page_des_new->slot_num = 0; 
        page_des_new->if_leaf = 1;

        int slot_num_first = page_des_old->slot_num / 2; 
        int slot_num_second = page_des_old->slot_num - slot_num_first;

        //deal with slot
        void* src_back = old_node + PAGE_SIZE - page_des_old->slot_num * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR; 
        int length_back = slot_num_second * SIZE_OF_SLOT_DESCRIPTOR;
        void* des_back = new_node + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR - length_back; 
        memmove(des_back, src_back, length_back); 

        //deal with data
        slot_descriptor* new_page_begin_slot =  (slot_descriptor*)(old_node + PAGE_SIZE - (slot_num_first + 1) * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
        void* src_front = old_node + new_page_begin_slot->offset;
        int length_front = page_des_old->offset - new_page_begin_slot->offset;
        memmove(new_node, src_front, length_front);

        //regular case
        page_des_old->slot_num = slot_num_first; 
        page_des_old->offset -= length_front;

        page_des_new->next_page = page_des_old->next_page;
        page_des_old->next_page = page_num_new;
           
        page_des_new->slot_num = slot_num_second;
        page_des_new->offset = length_front;

        for (int i = 1; i <= page_des_new->slot_num; i++)
        {
            slot_descriptor* current_slot = (slot_descriptor* )(new_node + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR - i * SIZE_OF_SLOT_DESCRIPTOR); 
            current_slot->offset -= new_page_begin_slot->offset;
        }
    }

    void push_up_key_to_root(char* new_root, const RID& rid_to_push_up, int current_page_num, int next_page_num, const int& key_to_push_up_int, const std::string& key_to_push_up_str, const Attribute& attribute)
    {
        page_descriptor* page_des_new_root = (page_descriptor* )(new_root + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 
        page_des_new_root->if_leaf = 0; 
        page_des_new_root->next_page = -1;
        page_des_new_root->offset = 0; 
        page_des_new_root->slot_num = 0; 

        slot_descriptor* slot_1 = (slot_descriptor* )(new_root + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR - SIZE_OF_SLOT_DESCRIPTOR * 1);
        slot_descriptor* slot_2 = (slot_descriptor* )(new_root + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR - SIZE_OF_SLOT_DESCRIPTOR * 2);

        //reflects the difference between 'push-up' & 'copy-up'
        //not accurate here... will change
        if (attribute.type == TypeInt || attribute.type == TypeReal)
            slot_1->length = SIZE_OF_INT;
        else
            slot_1->length = key_to_push_up_str.size();
        slot_1->offset = 0;
        slot_1->pointer = current_page_num;
        slot_1->rid = rid_to_push_up;

        slot_2->length = -1;
        slot_2->offset = -1;
        slot_2->pointer = next_page_num;
        slot_2->rid.pageNum = 0;
        slot_2->rid.slotNum = 0;

        //write the key to the root page
        if (attribute.type == TypeInt || attribute.type == TypeReal)
            memmove(new_root, &key_to_push_up_int, SIZE_OF_INT);
        else
            memmove(new_root, key_to_push_up_str.c_str(), key_to_push_up_str.size());
        
        page_des_new_root->slot_num = 2; 
        page_des_new_root->offset = slot_1->length;
    }
    void copy_up_key_to_root(char* new_root, char* new_leaf, int current_page_num, int new_leaf_page_num)
    {
        page_descriptor* page_des_new_root = (page_descriptor* )(new_root + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 
        page_descriptor* page_des_new_leaf = (page_descriptor* )(new_leaf + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR);
        slot_descriptor* new_page_begin_slot = (slot_descriptor* )(new_leaf + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR - SIZE_OF_SLOT_DESCRIPTOR);
        
        page_des_new_root->if_leaf = 0; 
        page_des_new_root->next_page = -1;
        page_des_new_root->offset = 0; 
        page_des_new_root->slot_num = 0; 

        slot_descriptor* slot_1 = (slot_descriptor* )(new_root + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR - SIZE_OF_SLOT_DESCRIPTOR * 1);
        slot_descriptor* slot_2 = (slot_descriptor* )(new_root + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR - SIZE_OF_SLOT_DESCRIPTOR * 2);

        //reflects the difference between 'push-up' & 'copy-up'
        slot_1->length = new_page_begin_slot->length;
        slot_1->offset = 0;
        slot_1->pointer = current_page_num;
        slot_1->rid = new_page_begin_slot->rid;
        
        slot_2->length = -1;
        slot_2->offset = -1;
        slot_2->pointer = new_leaf_page_num;
        slot_2->rid.pageNum = 0;
        slot_2->rid.slotNum = 0;

        /*
        if (attribute.type == TypeInt || attribute.type == TypeReal)
            key_to_push_up_int = *(int* )(new_leaf);
        else
            key_to_push_up_str.assign(new_leaf, new_page_begin_slot->length);
        rid_to_push_up = new_page_begin_slot->rid;
        */

        //write the key to the root page
        memmove(new_root, new_leaf, slot_1->length); 
        page_des_new_root->slot_num = 2; 
        page_des_new_root->offset = slot_1->length;
    }
    //Assume both old_node and new_node are pointing to a 4K page. Parameters [new_root] and [page_num_old] are only required if [special_case = 1].
    
    int rid_greater(const RID& rid1, const RID& rid2)
    {
        if (rid1.pageNum > rid2.pageNum)
            return 1;
        if (rid1.pageNum == rid2.pageNum && rid1.slotNum > rid2.slotNum)
            return 1;
        return 0;
    }

    int insert_entry_to_a_specific_page(IXFileHandle &ixFileHandle, const Attribute &attribute, int key_size, int key_val_int, std::string key_val_str, const RID &rid, char*& current_page, const int& current_page_num, int if_leaf, int ptr)
    {
        page_descriptor* page_des = (page_descriptor* )(current_page + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 

        int val_to_compare_int = -1; 
        std::string val_to_compare_str; 
            
        int allocated = 0; 
        slot_descriptor* current_slot = nullptr;
        
        //This case is basically done.
        if (if_leaf)
        {
            int i = 1; 
            
            /*
            if (page_des->slot_num == 0)
                i = 1;
-           else
-               i = binary_search(current_page, page_des->slot_num + 1, 1, key_val_int, key_val_str, rid, attribute); 
            */

            for (; i <= page_des->slot_num + 1; i++)
            {
                current_slot = (slot_descriptor* )(current_page + PAGE_SIZE - i * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
                if (i == page_des->slot_num + 1)
                    break;

                if (attribute.type == TypeInt || attribute.type == TypeReal)
                {
                    val_to_compare_int = *(int* )(current_page + current_slot->offset); 
                    if (val_to_compare_int > key_val_int || (val_to_compare_int == key_val_int && rid_greater(current_slot->rid, rid)))
                        break;
                    
                }
                else if (attribute.type == TypeVarChar)
                {
                    val_to_compare_str.assign((char* )current_page + current_slot->offset, current_slot->length);
                    if (val_to_compare_str > key_val_str || (val_to_compare_str == key_val_str && rid_greater(current_slot->rid, rid)))
                        break;
                }
            }
            
            current_slot = (slot_descriptor* )(current_page + PAGE_SIZE - i * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
            
            
            
            //make the hole in the back
            void* src_back = current_page + PAGE_SIZE - page_des->slot_num * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR; 
            void* des_back = (char* )src_back - SIZE_OF_SLOT_DESCRIPTOR; 
            int size_back = (char* )current_slot - (char* )des_back; 
            memmove(des_back, src_back, size_back);
            
            //change the corresponding meta-data
            current_slot->rid = rid; 
            current_slot->length = key_size;
            current_slot->pointer = ptr;
            //otherwise we don't change the offset of the current slot
            if (i == page_des->slot_num + 1)
                current_slot->offset = page_des->offset;
            
            for (int j = i + 1; j <= page_des->slot_num + 1; j++)
            {
                slot_descriptor* slot_to_change_offset = (slot_descriptor* )(current_page + PAGE_SIZE - j * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
                slot_to_change_offset->offset += key_size;
            }

            //make the hole in the front
            void* src_front = current_page + current_slot->offset; 
            void* des_front = (char* )src_front + current_slot->length; 
            int size_front = page_des->offset - current_slot->offset; 
            memmove(des_front, src_front, size_front); 

            //fill in the hole in the front
            if (attribute.type == TypeInt || attribute.type == TypeReal)
                memmove(current_page + current_slot->offset, &key_val_int, key_size);
            else
                memmove(current_page + current_slot->offset, key_val_str.c_str(), key_size);
        }

        else
        {
            int i = 1; 
            for (; i <= page_des->slot_num; i++)
            {
                current_slot = (slot_descriptor* )(current_page + PAGE_SIZE - i * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);

                if (i == page_des->slot_num)
                    break;

                if (attribute.type == TypeInt || attribute.type == TypeReal)
                {
                    val_to_compare_int = *(int* )(current_page + current_slot->offset); 
                    if (val_to_compare_int > key_val_int || (val_to_compare_int == key_val_int && rid_greater(current_slot->rid, rid)))
                        break;
                }
                else if (attribute.type == TypeVarChar)
                {
                    val_to_compare_str.assign((char* )current_page + current_slot->offset, current_slot->length);
                    if (val_to_compare_str > key_val_str || (val_to_compare_str == key_val_str && rid_greater(current_slot->rid, rid)))
                        break;
                }
            }

            //make the hole in the back
            void* src_back = current_page + PAGE_SIZE - page_des->slot_num * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR; 
            void* des_back = (char* )src_back - SIZE_OF_SLOT_DESCRIPTOR; 
            int size_back = (char* )current_slot - (char* )des_back; 
            memmove(des_back, src_back, size_back);
            
            //change the corresponding meta-data
            current_slot->rid = rid; 
            current_slot->length = key_size;
            (current_slot - 1)->pointer = ptr;
            if (i == page_des->slot_num)
                current_slot->offset = page_des->offset;

            for (int j = i + 1; j <= page_des->slot_num + 1; j++)
            {
                slot_descriptor* slot_to_change_offset = (slot_descriptor* )(current_page + PAGE_SIZE - j * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
                slot_to_change_offset->offset += key_size;
            }

            //make the hole in the front
            //if (i < page_des->slot_num)
            //{
                void* src_front = current_page + current_slot->offset; 
                void* des_front = (char* )src_front + current_slot->length; 
                int size_front = page_des->offset - current_slot->offset; 
                if (size_front != 0)
                    memmove(des_front, src_front, size_front); 
            //}
            
            //fill the hole in the front
            void* src_front_for_insertion = nullptr;
            if (attribute.type == TypeInt || attribute.type == TypeReal)
                src_front_for_insertion = &key_val_int;
            else
                src_front_for_insertion = (void* )key_val_str.c_str(); 
            
            void* des_front_for_insertion = current_page + current_slot->offset; 
            int size_front_for_insertion = current_slot->length;
            memmove(des_front_for_insertion, src_front_for_insertion, size_front_for_insertion);    
        }

        page_des->slot_num += 1; 
        page_des->offset += key_size;

        if (ixFileHandle.file_handle.writePage(current_page_num, current_page) == -1)
            return -1;
        return 0; 
    }

    int delete_on_a_specific_page(IXFileHandle& ixFileHandle, char* current_node, slot_descriptor* ret_slot, short current_page_num)
    {
        int length_of_deleted = ret_slot->length;
        
        page_descriptor* page_des_current_node = (page_descriptor* )(current_node + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 
        void* src_front = current_node + ret_slot->offset + ret_slot->length; 
        void* des_front = current_node + ret_slot->offset; 
        int size_front = page_des_current_node->offset - (ret_slot->offset + ret_slot->length); 
        memmove(des_front, src_front, size_front); 

        void* src_back = current_node + PAGE_SIZE - page_des_current_node->slot_num * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR; 
        void* des_back = (char* )src_back + SIZE_OF_SLOT_DESCRIPTOR; 
        int size_back = (char* )ret_slot - (char* )src_back;
        memmove(des_back, src_back, size_back);
        
        page_des_current_node->offset -= length_of_deleted; 
        page_des_current_node->slot_num -= 1;

        for (slot_descriptor* current_slot = ret_slot; current_slot >= (slot_descriptor* )des_back; current_slot--)
        {
            current_slot->offset -= length_of_deleted; 
        }

        return ixFileHandle.file_handle.writePage(current_page_num, current_node);
    }

    int locate(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, char*& current_node, slot_descriptor*& ret_slot, short& page_num, const RID& rid)
    {
        page_descriptor* page_des_current_node = (page_descriptor* )(current_node + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 
        int key_size = -1; 
        if (attribute.type == TypeInt || attribute.type == TypeReal)
            key_size = 4;
        else if (attribute.type == TypeVarChar)
            key_size = *(int* )key; 
        
        int key_val_int = -1; 
        std::string key_val_str; 
        //leaf_slot* slot_des_leaf = nullptr;

        int val_to_compare_int = -1; 
        std::string val_to_compare_str; 

        if (attribute.type == TypeInt || attribute.type == TypeReal)
            key_val_int = *(int* )key; 
        else if (attribute.type == TypeVarChar)
            key_val_str.assign(SIZE_OF_INT + (char* )key, key_size);
        
        if (page_des_current_node->if_leaf == 0)
        {
            slot_descriptor* current_slot = nullptr;
            //slot_descriptor* prev_slot = (slot_descriptor* )(current_node + PAGE_SIZE - SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);

            for (int i = 1; i <= page_des_current_node->slot_num; i++)
            {
                current_slot = (slot_descriptor* )(current_node + PAGE_SIZE - i * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);

                if (i == page_des_current_node->slot_num)
                    break;

                if (attribute.type == TypeInt || attribute.type == TypeReal)
                {
                    val_to_compare_int = *(int* )(current_node + current_slot->offset); 
                    if (val_to_compare_int > key_val_int || (val_to_compare_int == key_val_int && rid_greater(current_slot->rid, rid)))
                        break;
                }
                else if (attribute.type == TypeVarChar)
                {
                    val_to_compare_str.assign((char* )current_node + current_slot->offset, current_slot->length);
                    if (val_to_compare_str > key_val_str || (val_to_compare_str == key_val_str && rid_greater(current_slot->rid, rid)))
                        break;
                }
            }

            short page_num_next = current_slot->pointer;
            free(current_node);

            char* next_node = (char* )malloc(PAGE_SIZE);
            if (ixFileHandle.file_handle.readPage(page_num_next, next_node) == -1)
            {
                free(next_node);
                return -1;
            }

            current_node = next_node; 
            page_num = page_num_next;
            if (locate(ixFileHandle, attribute, key, current_node, ret_slot, page_num, rid) == -1)
            {
                return -1;
            }
            return 0;
        }

        //else -> leaf_node
        else
        {
            slot_descriptor* current_slot = nullptr;
            int found_flag = 0;
            for (int i = 1; i <= page_des_current_node->slot_num; i++)
            {
                current_slot = (slot_descriptor* )(current_node + PAGE_SIZE - i * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
                
                if (attribute.type == TypeInt || attribute.type == TypeReal)
                {
                    val_to_compare_int = *(int* )(current_node + current_slot->offset); 
                    if (val_to_compare_int == key_val_int)
                    {
                        found_flag = 1; 
                        break;
                    }
                    else if (val_to_compare_int > key_val_int)
                    {
                        found_flag = 0;
                        break;
                    }
                }
                else if (attribute.type == TypeVarChar)
                {
                    val_to_compare_str.assign((char* )current_node + current_slot->offset, current_slot->length);
                    if (val_to_compare_str == key_val_str)
                    {
                        found_flag = 1; 
                        break;
                    }
                    else if (val_to_compare_str > key_val_str)
                    {
                        found_flag = 0; 
                        break;
                    }
                }
            }
            if (found_flag == 1)
            {
                ret_slot = current_slot;
                return 0;
            }
            else
                return -1;
        }
    }

    int key_insert(IXFileHandle &ixFileHandle, const Attribute &attribute, const int& key_size, const int& key_int, const std::string& key_str, const RID &rid, char*& current_page, int current_page_num, int& key_to_push_up_int, std::string& key_to_push_up_str, RID& rid_to_push_up)
    {
        page_descriptor* page_des = (page_descriptor* )(current_page + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 
        int val_to_compare_int = -1; 
        std::string val_to_compare_str; 

        if (page_des->if_leaf == 0)
        {
            //always: the insert part
            slot_descriptor* current_slot = nullptr;
            for (int i = 1; i <= page_des->slot_num; i++)
            {
                current_slot = (slot_descriptor* )(current_page + PAGE_SIZE - i * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
                if (i == page_des->slot_num)
                    break;
                if (attribute.type == TypeInt || attribute.type == TypeReal)
                {
                    val_to_compare_int = *(int* )(current_page + current_slot->offset); 
                    if (val_to_compare_int > key_int || (val_to_compare_int == key_int && rid_greater(current_slot->rid, rid)))
                        break;
                }
                else if (attribute.type == TypeVarChar)
                {
                    val_to_compare_str.assign((char* )current_page + current_slot->offset, current_slot->length);
                    if (val_to_compare_str > key_str || (val_to_compare_str == key_str && rid_greater(current_slot->rid, rid)))
                        break;
                }
            }
            int page_num_next = current_slot->pointer;
            char* next_node = (char* )malloc(PAGE_SIZE);
            if (ixFileHandle.file_handle.readPage(page_num_next, next_node) == -1)
            {
                free(next_node);
                return -1;
            }

            int rc = key_insert(ixFileHandle, attribute, key_size, key_int, key_str, rid, next_node, page_num_next, key_to_push_up_int, key_to_push_up_str, rid_to_push_up);
            
            //unusual: the push_up part
            if (rc > 0)
            {
                //insert them now!
                int key_size_to_insert = -1;
                if (attribute.type == TypeInt || attribute.type == TypeReal)
                    key_size_to_insert = key_size;  
                else
                    key_size_to_insert = key_to_push_up_str.size();

                RID rid_to_insert = rid_to_push_up;
                int val_to_insert_int = key_to_push_up_int; 
                std::string val_to_insert_str = key_to_push_up_str;

                //next node is no longer useful to us
                free(next_node);
                next_node = nullptr;

                //if the newly pushed-up key can fit the page
                //unusual: it can't, so we need to split!
                if (!if_new_entry_fit(page_des, key_size))
                {
                    //allocate a page
                    if (ixFileHandle.file_handle.appendPage(nullptr) == -1)
                        return -1;
                    int next_page_num = ixFileHandle.file_handle.getNumberOfPages() - 1;
                    char* next_page = (char* )malloc(PAGE_SIZE); 
                    if (ixFileHandle.file_handle.readPage(next_page_num, next_page) == -1)
                    {
                        free(next_page);
                        return -1;
                    }

                    //use our unique push_up function...! all 'push_up' items should be modified!
                    split_push_up(current_page, next_page, key_to_push_up_int, key_to_push_up_str, rid_to_push_up, attribute);
                    //determine which one of the new/old node to insert and insert the key
                    int insert_new = 1;
                    if (attribute.type == TypeInt || attribute.type == TypeReal)
                    {
                        if (key_to_push_up_int > val_to_insert_int || (key_to_push_up_int == val_to_insert_int && rid_greater(rid_to_push_up, rid_to_insert)))
                            insert_new = 0;
                    }
                    else if (attribute.type == TypeVarChar)
                    {
                        if (key_to_push_up_str > val_to_insert_str || (key_to_push_up_str == val_to_insert_str && rid_greater(rid_to_push_up, rid_to_insert)))
                            insert_new = 0;
                    }

                    if (insert_new)
                    {
                        if (insert_entry_to_a_specific_page(ixFileHandle, attribute, key_size_to_insert, val_to_insert_int, val_to_insert_str, rid_to_insert, next_page, next_page_num, 0, rc) == -1)
                        {
                            free(next_page);
                            return -1;
                        }
                    }
                    else
                    {
                        if (insert_entry_to_a_specific_page(ixFileHandle, attribute, key_size_to_insert, val_to_insert_int, val_to_insert_str, rid_to_insert, current_page, current_page_num, 0, rc) == -1)
                        {
                            free(next_page);
                            return -1;
                        }
                    }
                    
                    //write both the [current page] and the [next page] into disk
                    if (ixFileHandle.file_handle.writePage(current_page_num, current_page) == -1)
                    {
                        free(next_page);
                        return -1;
                    }
                    if (ixFileHandle.file_handle.writePage(next_page_num, next_page) == -1)
                    {
                        free(next_page);
                        return -1;
                    }

                    //do extra work if the current node is root!
                    if (current_page_num == ixFileHandle.read_root_page_num())
                    {
                        if (ixFileHandle.file_handle.appendPage(nullptr) == -1)
                        {
                            free(next_page);
                            return -1;
                        }
                        char* new_root = (char* )malloc(PAGE_SIZE);
                        push_up_key_to_root(new_root, rid_to_push_up, current_page_num, next_page_num, key_to_push_up_int, key_to_push_up_str, attribute);
                        
                        if (ixFileHandle.write_root_page_num(ixFileHandle.file_handle.getNumberOfPages() - 1) == -1)
                        {
                            free(next_page);
                            free(new_root);
                            return -1;
                        }
                        if (ixFileHandle.file_handle.writePage(ixFileHandle.file_handle.getNumberOfPages() - 1, new_root) == -1)
                        {
                            free(next_page);
                            free(new_root);
                            return -1;
                        }
                        free(next_page); 
                        free(new_root); 
                        return 0;
                    }
                    //return a page_num since we splited
                    else
                    {
                        free(next_page); 
                        return next_page_num;
                    }
                }
                else
                    //the code should be exactly same as above
                    return insert_entry_to_a_specific_page(ixFileHandle, attribute, key_size_to_insert, val_to_insert_int, val_to_insert_str, rid_to_insert, current_page, current_page_num, 0, rc);
            }
            else 
                return 0;
        }
            //leaf case
        else
        {
            //unusual: split!
            if (!if_new_entry_fit(page_des, key_size))
            {
                if (ixFileHandle.file_handle.appendPage(nullptr) == -1)
                    return -1;
                
                int new_leaf_page_num = ixFileHandle.file_handle.getNumberOfPages() - 1;
                char* new_leaf = (char* )malloc(PAGE_SIZE); 
                if (ixFileHandle.file_handle.readPage(new_leaf_page_num, new_leaf) == -1)
                {
                    free(new_leaf); 
                    return -1;
                }

                split_copy_up(current_page, new_leaf, new_leaf_page_num);
                //the following code determines whether to insert the new entry to the old node or to the new node
                //and, inserts the new entry
                int insert_new = 1;
                slot_descriptor* new_page_begin_slot =  (slot_descriptor*)(new_leaf + PAGE_SIZE - SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
                if (attribute.type == TypeInt || attribute.type == TypeReal)
                {
                    val_to_compare_int = *(int* )(new_leaf + new_page_begin_slot->offset); 
                    if (val_to_compare_int > key_int || (val_to_compare_int == key_int && rid_greater(new_page_begin_slot->rid, rid)))
                        insert_new = 0;
                }
                else if (attribute.type == TypeVarChar)
                {
                    val_to_compare_str.assign((char* )new_leaf + new_page_begin_slot->offset, new_page_begin_slot->length);
                    if (val_to_compare_str > key_str || (val_to_compare_str == key_str && rid_greater(new_page_begin_slot->rid, rid)))
                        insert_new = 0;
                }

                if (insert_new)
                {
                    if (insert_entry_to_a_specific_page(ixFileHandle, attribute, key_size, key_int, key_str, rid, new_leaf, new_leaf_page_num, 1, 0) == -1)
                    {
                        free(new_leaf);
                        return -1;
                    }
                }
                else
                {
                    if (insert_entry_to_a_specific_page(ixFileHandle, attribute, key_size, key_int, key_str, rid, current_page, current_page_num, 1, 0) == -1)
                    {
                        free(new_leaf);
                        return -1;
                    }
                }

                //write both the [current page] and the [next page] into disk
                if (ixFileHandle.file_handle.writePage(current_page_num, current_page) == -1)
                {
                    free(new_leaf);
                    return -1;
                }
                if (ixFileHandle.file_handle.writePage(new_leaf_page_num, new_leaf) == -1)
                {
                    free(new_leaf);
                    return -1;
                }
                //slot_descriptor* first_slot_on_new_leaf = (slot_descriptor* )(new_leaf + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR - SIZE_OF_SLOT_DESCRIPTOR); 
                key_to_push_up_int = *(int* )new_leaf;
                key_to_push_up_str.assign(new_leaf, new_page_begin_slot->length);
                rid_to_push_up = new_page_begin_slot->rid;
                
                if (current_page_num == ixFileHandle.read_root_page_num())
                {
                    //do the special split
                    if (ixFileHandle.file_handle.appendPage(nullptr) == -1)
                    {
                        free(new_leaf);
                        return -1;
                    }
                    char* new_root = (char* )malloc(PAGE_SIZE);
                    copy_up_key_to_root(new_root, new_leaf, current_page_num, new_leaf_page_num);
                    if (ixFileHandle.write_root_page_num(ixFileHandle.file_handle.getNumberOfPages() - 1) == -1)
                    {
                        free(new_leaf);
                        free(new_root);
                        return -1;
                    }
                    if (ixFileHandle.file_handle.writePage(ixFileHandle.file_handle.getNumberOfPages() - 1, new_root) == -1)
                    {
                        free(new_leaf);
                        free(new_root);
                        return -1;
                    }
                    free(new_root);
                    free(new_leaf);
                    return 0; 
                }
                else
                {
                    free(new_leaf);
                    return new_leaf_page_num;
                }     
            }
            else
                return insert_entry_to_a_specific_page(ixFileHandle, attribute, key_size, key_int, key_str, rid, current_page, current_page_num, 1, 0); 
        }
    }

    RC IndexManager::createFile(const std::string &fileName) {
        return PagedFileManager::instance().createFile(fileName);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::instance().destroyFile(fileName); 
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        return PagedFileManager::instance().openFile(fileName, ixFileHandle.file_handle); 
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return PagedFileManager::instance().closeFile(ixFileHandle.file_handle); 
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        int key_size = -1; 
        if (attribute.type == TypeInt || attribute.type == TypeReal)
            key_size = 4;
        else if (attribute.type == TypeVarChar)
            key_size = *(int* )key; 
        
        int key_val_int = -1; 
        std::string key_val_str; 

        int val_to_compare_int = -1; 
        std::string val_to_compare_str; 

        if (attribute.type == TypeInt || attribute.type == TypeReal)
            key_val_int = *(int* )key; 
        else if (attribute.type == TypeVarChar)
            key_val_str.assign(SIZE_OF_INT + (char* )key, key_size);

        int key_to_push_up_int; 
        std::string key_to_push_up_str;
        RID rid_to_push_up;
        
        if (ixFileHandle.file_handle.getNumberOfPages() == 0)
        {
            char* pointer_page = (char* )malloc(PAGE_SIZE); 
            *(int* )pointer_page = 1; 

            if (ixFileHandle.file_handle.appendPage(pointer_page) == -1)
            {
                free(pointer_page); 
                return -1; 
            }
            free(pointer_page);

            char* leaf = (char* )malloc(PAGE_SIZE); 
            page_descriptor* page_des_leaf = (page_descriptor* )(leaf + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 
            page_des_leaf->next_page = -1; 
            page_des_leaf->offset = 0; 
            page_des_leaf->slot_num = 0; 
            page_des_leaf->if_leaf = 1;
            if (ixFileHandle.file_handle.appendPage(leaf) == -1)
            {
                free(leaf); 
                return -1; 
            }
            //char* temp = (char* )malloc(PAGE_SIZE); 
            if (key_insert(ixFileHandle, attribute, key_size, key_val_int, key_val_str, rid, leaf, 1, key_to_push_up_int, key_to_push_up_str, rid_to_push_up) == -1)
            {
                free(leaf); 
                //free(temp);
                return -1;
            }
            /*
            if (insert(ixFileHandle, attribute, key, rid, leaf, 1) == -1)
            {
                free(leaf); 
                //free(temp);
                return -1;
            }
            */
            free(leaf); 
            //free(temp);
            return 0;
        }

        char* root_ptr_page = (char* )malloc(PAGE_SIZE); 
        if (ixFileHandle.file_handle.readPage(0, root_ptr_page) == -1)
        {
            free(root_ptr_page); 
            return -1;
        }

        int root_ptr = *(int* )root_ptr_page;
        free(root_ptr_page);

        char* root_page = (char* )malloc(PAGE_SIZE);
        if (ixFileHandle.file_handle.readPage(root_ptr, root_page) == -1)
        {
            free(root_page); 
            return -1;
        }

        //char* temp = (char* )malloc(PAGE_SIZE); 
        /*
        if (insert(ixFileHandle, attribute, key, rid, root_page, root_ptr) == -1)
            return -1;
        else
            return 0;
        */
        

        if (key_insert(ixFileHandle, attribute, key_size, key_val_int, key_val_str, rid, root_page, root_ptr, key_to_push_up_int, key_to_push_up_str, rid_to_push_up) == -1)
        {
            free(root_page);
            return -1; 
        }
        free(root_page); 
        return 0; 
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        char* root_ptr_page = (char* )malloc(PAGE_SIZE); 
        if (ixFileHandle.file_handle.readPage(0, root_ptr_page) == -1)
        {
            free(root_ptr_page); 
            return -1;
        }

        int root_ptr = *(int* )root_ptr_page;
        free(root_ptr_page);

        char* root_page = (char* )malloc(PAGE_SIZE);
        if (ixFileHandle.file_handle.readPage(root_ptr, root_page) == -1)
        {
            free(root_page); 
            return -1;
        }

        slot_descriptor* ret_leaf_slot = nullptr;
        short ret_page_num = root_ptr; 

        if (locate(ixFileHandle, attribute, key, root_page, ret_leaf_slot, ret_page_num, rid) == -1)
        {
            //can't free root
            free(root_page);
            return -1;
        }
        
        return delete_on_a_specific_page(ixFileHandle, root_page, ret_leaf_slot, ret_page_num);
    }

    int locate_page(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, char* current_node, short& page_num, int if_neg_infinity);

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        
        ix_ScanIterator.ixFileHandle = ixFileHandle;
        ix_ScanIterator.attribute = attribute;
        ix_ScanIterator.lowKey = lowKey; 
        ix_ScanIterator.highKey = highKey; 
        ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
        ix_ScanIterator.highKeyInclusive = highKeyInclusive;
        ix_ScanIterator.found_flag = 0;
        ix_ScanIterator.if_neg_infinity = 0; 
        ix_ScanIterator.if_pos_infinity = 0;
        ix_ScanIterator.current_page = nullptr;

        if (lowKey == nullptr)
            ix_ScanIterator.if_neg_infinity = 1;
        if (highKey == nullptr)
            ix_ScanIterator.if_pos_infinity = 1;

        if (attribute.type == TypeInt || attribute.type == TypeReal)
        {
            if (!ix_ScanIterator.if_neg_infinity)
                ix_ScanIterator.low_key_int = *(int* )lowKey;
            if (!ix_ScanIterator.if_pos_infinity)
                ix_ScanIterator.high_key_int = *(int* )highKey; 
        }
        else if (attribute.type == TypeVarChar)
        {
            if (!ix_ScanIterator.if_neg_infinity)
            {
                int low_key_size = *(int* )lowKey; 
                ix_ScanIterator.low_key_str.assign(SIZE_OF_INT + (char* )lowKey, low_key_size);
            }
            if (!ix_ScanIterator.if_pos_infinity)
            {
                int high_key_size = *(int* )highKey; 
                ix_ScanIterator.high_key_str.assign(SIZE_OF_INT + (char* )highKey, high_key_size);
            }
        }
        
        int root_ptr;
        root_ptr = ixFileHandle.read_root_page_num();
        
        if (root_ptr == -1)
            return -1;

        char* root_page = (char* )malloc(PAGE_SIZE);
        if (ixFileHandle.file_handle.readPage(root_ptr, root_page) == -1)
        {
            free(root_page); 
            return -1;
        }

        slot_descriptor* ret_leaf_slot = nullptr;
        short ret_page_num = root_ptr; 
        
        if (locate_page(ixFileHandle, attribute, lowKey, root_page, ret_page_num, ix_ScanIterator.if_neg_infinity) == -1)
        {
            //can't free root
            free(root_page);
            return -1;
        }
        free(root_page);

        ix_ScanIterator.current_page_num = ret_page_num;
        ix_ScanIterator.current_slot_num = 0;
        
        ix_ScanIterator.current_page = malloc(PAGE_SIZE);
        if (ixFileHandle.file_handle.readPage(ix_ScanIterator.current_page_num, ix_ScanIterator.current_page) == -1)
        {
            free(ix_ScanIterator.current_page);
            return -1;
        }
        return 0; 
    }

    int sub_print(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out, int page_num)
    {
        char* current_page = (char* )malloc(PAGE_SIZE); 
        if (ixFileHandle.file_handle.readPage(page_num, current_page) == -1)
        {
            free(current_page); 
            return -1;
        }
        page_descriptor* page_des_current_node = (page_descriptor* )(current_page + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 
        if (page_des_current_node->if_leaf == 0)
        {
            slot_descriptor* current_slot = nullptr;
            out<<"{\"keys\":";
            out<<"[";
            for (int i = 1; i <= page_des_current_node->slot_num - 1; i++)
            {
                current_slot = (slot_descriptor* )(current_page + PAGE_SIZE - i * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
                if (attribute.type == TypeInt)
                {
                    int key_int = *(int* )(current_page + current_slot->offset);
                    out<<key_int; 
                }
                else if (attribute.type == TypeReal)
                {
                    float key_float = *(float* )(current_page + current_slot->offset);
                    out<<key_float; 
                }
                else if (attribute.type == TypeVarChar)
                {
                    std::string key_varchar; 
                    key_varchar.assign((char* )current_page + current_slot->offset, current_slot->length);
                    out<<"\""<<key_varchar<<"\"";
                }
                if (i != page_des_current_node->slot_num - 1)
                    out<<",";
            }
            out<<"],";
            out<<"\"children\":";
            out<<"[";
            for (int i = 1; i <= page_des_current_node->slot_num; i++)
            {
                current_slot = (slot_descriptor* )(current_page + PAGE_SIZE - i * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
                sub_print(ixFileHandle, attribute, out, current_slot->pointer);
                if (i != page_des_current_node->slot_num)
                    out<<",";
            }
            out<<"]";
            out<<"}";
        }
        else
        {
            slot_descriptor* current_slot = nullptr;
            int key_int_prev;
            float key_float_prev;
            std::string key_varchar_prev;

            out<<"{\"keys\":";
            out<<"[";

            for (int i = 1; i <= page_des_current_node->slot_num; i++)
            {
                current_slot = (slot_descriptor* )(current_page + PAGE_SIZE - i * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
                if (attribute.type == TypeInt)
                {
                    int key_int = *(int* )(current_page + current_slot->offset);
                    if (i != 1 && key_int == key_int_prev)
                    {
                        out<<",";
                    }
                    else
                    {
                        //["A:[(1,1),(1,2)]"
                        if (i != 1)
                            out<<"]"<<"\"";
                        out<<"\""<<key_int<<":"<<"["<<",";
                    }
                    out<<"("<<current_slot->rid.pageNum<<","<<current_slot->rid.slotNum<<")";
                    key_int_prev = key_int;
                }
                else if (attribute.type == TypeReal)
                {
                    float key_float = *(float* )(current_page + current_slot->offset);
                    if (i != 1 && key_float == key_float_prev)
                    {
                        out<<",";
                    }
                    else
                    {
                        if (i != 1)
                            out<<"]"<<"\""<<",";
                        out<<"\""<<key_float<<":"<<"[";
                    }
                    out<<"("<<current_slot->rid.pageNum<<","<<current_slot->rid.slotNum<<")";
                    key_float_prev = key_float;
                }
                else if (attribute.type == TypeVarChar)
                {
                    std::string key_varchar; 
                    key_varchar.assign((char* )current_page + current_slot->offset, current_slot->length);
                    if (i != 1 && key_varchar == key_varchar_prev)
                    {
                        out<<",";
                    }
                    else
                    {
                        if (i != 1)
                            out<<"]"<<"\""<<",";
                        out<<"\""<<key_varchar<<":"<<"[";
                    }
                    out<<"("<<current_slot->rid.pageNum<<","<<current_slot->rid.slotNum<<")";
                    key_varchar_prev = key_varchar;
                }
                
                //if (i != page_des_current_node->slot_num)
                //    out<<",";
                
            }
            if (page_des_current_node->slot_num > 0)
                out<<"]"<<"\"";
            out<<"]";
            out<<"}";
        }
        free(current_page);
        return 0; 
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        //out = std::cout;
        char* pointer_page = (char* )malloc(PAGE_SIZE); 
        if (ixFileHandle.file_handle.readPage(0, pointer_page) == -1)
        {
            free(pointer_page);
            return -1;
        }
        
        int root = *(int* )(pointer_page); 
        if (sub_print(ixFileHandle, attribute, out, root) == -1)
            return -1;
        //someFunc(out);
        return 0;
    }

    IX_ScanIterator::IX_ScanIterator() {
        found_flag = 0;
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    int locate_page(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, char* current_node, short& page_num, int if_neg_infinity)
    {
        page_descriptor* page_des_current_node = (page_descriptor* )(current_node + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR); 
        int key_size = -1; 
        
        int key_val_int = -1; 
        std::string key_val_str; 
        //leaf_slot* slot_des_leaf = nullptr;

        int val_to_compare_int = -1; 
        std::string val_to_compare_str; 

        if (!if_neg_infinity)
        {
            if (attribute.type == TypeInt || attribute.type == TypeReal)
                key_size = 4;
            else if (attribute.type == TypeVarChar)
                key_size = *(int* )key; 
            if (attribute.type == TypeInt || attribute.type == TypeReal)
                key_val_int = *(int* )key; 
            else if (attribute.type == TypeVarChar)
                key_val_str.assign(SIZE_OF_INT + (char* )key, key_size);
        }
        
        if (page_des_current_node->if_leaf == 0)
        {
            slot_descriptor* current_slot = nullptr;
            //slot_descriptor* prev_slot = (slot_descriptor* )(current_node + PAGE_SIZE - SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);

            for (int i = 1; i <= page_des_current_node->slot_num; i++)
            {
                current_slot = (slot_descriptor* )(current_node + PAGE_SIZE - i * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);

                if (if_neg_infinity)
                    break;
                
                if (i == page_des_current_node->slot_num)
                    break;

                if (i == page_des_current_node->slot_num)
                    break;

                if (attribute.type == TypeInt || attribute.type == TypeReal)
                {
                    val_to_compare_int = *(int* )(current_node + current_slot->offset); 
                    if (val_to_compare_int >= key_val_int)
                        break; 
                }
                else if (attribute.type == TypeVarChar)
                {
                    val_to_compare_str.assign((char* )current_node + current_slot->offset, current_slot->length);
                    if (val_to_compare_str >= key_val_str) 
                        break;
                }
                //prev_slot = current_slot;
            }

            short page_num_next = current_slot->pointer;
            //free(current_node);

            char* next_node = (char* )malloc(PAGE_SIZE);
            if (ixFileHandle.file_handle.readPage(page_num_next, next_node) == -1)
            {
                free(next_node);
                return -1;
            }

            current_node = next_node; 
            page_num = page_num_next;
            if (locate_page(ixFileHandle, attribute, key, current_node, page_num, if_neg_infinity) == -1)
            {
                return -1;
            }
            return 0;
        }
        //else -> leaf_node
        else
            return 0;
    }
    

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        /*
        RID next_rid;
        next_rid.pageNum = -1; 
        next_rid.slotNum = -1; 
        */
        slot_descriptor* current_slot = nullptr;

        int val_to_compare_int = -1; 
        std::string val_to_compare_str; 

        /*
        char* current_page = (char* )malloc(PAGE_SIZE); 
        if (ixFileHandle.file_handle.readPage(current_page_num, current_page) == -1)
        {
            free(current_page);
            return -1;
        }
        */

        page_descriptor* page_des_current_page = (page_descriptor* )(current_page + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR);
        
        int slot_num_tmp = current_slot_num + 1;
        int page_num_tmp = current_page_num;
        
        while (1)
        {
            for (int i = slot_num_tmp; i <= page_des_current_page->slot_num; i++)
            {
                current_slot = (slot_descriptor* )(current_page + PAGE_SIZE - i * SIZE_OF_SLOT_DESCRIPTOR - SIZE_OF_PAGE_DESCRIPTOR);
                
                if (attribute.type == TypeInt || attribute.type == TypeReal)
                {
                    val_to_compare_int = *(int* )(current_page + current_slot->offset); 
                    if (found_flag == 0)
                    {
                        if (if_neg_infinity || val_to_compare_int > low_key_int || (val_to_compare_int == low_key_int && lowKeyInclusive))
                        {
                            if (if_pos_infinity || val_to_compare_int < high_key_int || (val_to_compare_int == high_key_int && highKeyInclusive))
                            {
                                found_flag = 1;
                                *(int* )key = val_to_compare_int; 
                                rid = current_slot->rid;
                                
                                current_page_num = page_num_tmp;
                                current_slot_num = i; 
                                
                                //free(current_page);
                                return 0; 
                            }
                            else
                            {
                                //free(current_page);
                                return -1;
                            }
                        }
                    }
                    else if (found_flag == 1)
                    {
                        if (if_pos_infinity || val_to_compare_int < high_key_int || (val_to_compare_int == high_key_int && highKeyInclusive))
                        {
                            *(int* )key = val_to_compare_int; 
                            rid = current_slot->rid;
                            
                            current_page_num = page_num_tmp;
                            current_slot_num = i; 
                            
                            //std::cout<<current_page_num<<","<<current_slot_num<<std::endl;
                            //free(current_page);
                            return 0; 
                        }
                        else
                        {
                            //free(current_page);
                            return -1;
                        }
                    }
                }
                else if (attribute.type == TypeVarChar)
                {
                    val_to_compare_str.assign((char* )current_page + current_slot->offset, current_slot->length);
                    if (found_flag == 0)
                    {
                        if (if_neg_infinity || val_to_compare_str > low_key_str || (val_to_compare_str == low_key_str && lowKeyInclusive))
                        {
                            if (if_pos_infinity || val_to_compare_str < high_key_str || (val_to_compare_str == high_key_str && highKeyInclusive))
                            {
                                found_flag = 1;
                                *(int* )key = current_slot->length;
                                memmove(key + SIZE_OF_INT, current_page + current_slot->offset, current_slot->offset);
                                rid = current_slot->rid;
                                
                                current_page_num = page_num_tmp;
                                current_slot_num = i; 
                                
                                //free(current_page);
                                return 0; 
                            }
                            else
                            {   
                                //free(current_page);
                                return -1;
                            }
                        }
                    }
                    else if (found_flag == 1)
                    {
                        if (if_pos_infinity || val_to_compare_str < high_key_str || (val_to_compare_str == high_key_str && highKeyInclusive))
                        {
                            found_flag = 1;
                            *(int* )key = current_slot->length;
                            memmove(key + SIZE_OF_INT, current_page + current_slot->offset, current_slot->offset);
                            rid = current_slot->rid;
                            
                            current_page_num = page_num_tmp;
                            current_slot_num = i; 

                            //free(current_page);
                            return 0; 
                        }
                        else
                        {   
                            //free(current_page);
                            return -1;
                        }
                    }
                }
            }
            if (page_des_current_page->next_page == -1)
            {
                //free(current_page);
                return -1;
            }
            page_num_tmp = page_des_current_page->next_page;
            if (ixFileHandle.file_handle.readPage(page_num_tmp, current_page) == -1)
            {
                //free(current_page);
                return -1;
            }
            slot_num_tmp = 1;
            page_descriptor* page_des_current_page = (page_descriptor* )(current_page + PAGE_SIZE - SIZE_OF_PAGE_DESCRIPTOR);
        }
    }

    RC IX_ScanIterator::close() {
        /*
        if (current_page != nullptr)
            free(current_page);
        */
        return 0;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        file_handle.collectCounterValues(readPageCount, writePageCount, appendPageCount); 
        return 0; 
    }

} // namespace PeterDB