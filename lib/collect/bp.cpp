#include <collect/bp.hpp>

constexpr size_t BIGROWLENTHRESHOLD = 32 * 1024; // 32KB


CBPlusTree::CBPlusTree(CPage& pge, CDiskMan& cdm, size_t pageSize, uint8_t& treeHigh, bidx& root, bidx& begin, bidx& end, uint16_t okeyLen, uint32_t orowLen, const std::vector<std::pair<uint8_t, dpfs_datatype_t>>& cmpTypes)
    :   m_page(pge),
        m_diskman(cdm),
        m_pageSize(pageSize),
        high(&treeHigh),
        m_root(&root),
        m_begin(&begin),
        m_end(&end),
        cmpTyps(cmpTypes) {


    // if table change is happend, should change the tree structure
    keyLen = okeyLen;
    m_rowLen = orowLen;

    maxkeyCount = ((m_pageSize * dpfs_lba_size - sizeof(NodeData::nd::hdr_t)) - sizeof(uint64_t)) / (keyLen + sizeof(uint64_t));
    // if row len is toooooooooooo big, change order to smaller value max block = 256MB
    if (m_rowLen > BIGROWLENTHRESHOLD * 8) {
        if (maxkeyCount > 14) maxkeyCount = 14;
    } else if (m_rowLen > BIGROWLENTHRESHOLD * 4) {
        if (maxkeyCount > 30) maxkeyCount = 30;
    } else if (m_rowLen > BIGROWLENTHRESHOLD * 2) {
        if (maxkeyCount > 60) maxkeyCount = 60;
    } else if (m_rowLen > BIGROWLENTHRESHOLD) {
        if (maxkeyCount > 120) maxkeyCount = 120;
    }


    // if (m_rowLen > MAXROWLEN) {
    //     // should not happen
    //     throw std::runtime_error("Row length exceeds maximum allowed size in BPlusTree constructor");
    // }

    // reserve 1 for split
    m_indexOrder = static_cast<size_t>(maxkeyCount) - 1;
    m_rowOrder = m_indexOrder; // ((m_pageSize * dpfs_lba_size - hdrSize) / (keyLen + m_rowLen)) - 1;

    // m_leafRowCount = (m_pageSize * dpfs_lba_size - hdrSize) / (keyLen + m_rowLen);
    size_t rowLenInByte = m_indexOrder * (keyLen + m_rowLen) + sizeof(NodeData::nd::hdr_t);

    
    
    m_rowPageSize = static_cast<uint8_t>( rowLenInByte % dpfs_lba_size == 0 ? rowLenInByte / dpfs_lba_size : (rowLenInByte / dpfs_lba_size) + 1 );

#ifdef __BPDEBUG__

    cout << "BPlusTree initialized with page size: " << (int)m_pageSize << " row page size: " << (int)m_rowPageSize << " LBA, key length: " << (int)keyLen 
    << ", index order: " << (int)m_indexOrder << ", row order: " << (int)m_rowOrder << ", row length: " << m_rowLen << endl;

    m_indexOrder = indOrder;
    m_rowOrder = rowOrder;
#endif

}

CBPlusTree::~CBPlusTree() {

    this->commit();

    // if (m_commitCache.size() > 0) {
    //     // rollback uncommitted nodes
    //     for(auto& it : m_commitCache) {
    //         #ifdef __BPDEBUG__
    //         cout << "Rolling back node gid: " << it.first.gid << " bid: " << it.first.bid << endl;
    //         #endif

    //         if (it.second.newNode) {
    //             // new node, free disk space
    //             int rc = free_node(it.first, it.second.nodeData->hdr->leaf);
    //             if (rc != 0) {
    //                 #ifdef __BPDEBUG__
    //                 cout << "Free node fail during destructor, gid: " << it.first.gid << " bid: " << it.first.bid << endl;
    //                 #endif
    //             }
    //         }

    //         // to prevent throw exception in destructor, if lock fail, force to continue
    //         int rc = it.second.pCache->lock();
    //         if (rc != 0) {
    //             // lock fail, force set to invalid 
    //             it.second.pCache->setStatus(cacheStruct::INVALID);
    //             #ifdef __BPDEBUG__
    //             cout << "Lock fail during destructor, force set node to invalid, gid: " << it.first.gid << " bid: " << it.first.bid << endl;
    //             abort();
    //             #endif  
    //         } else {
    //             // lock success, set and unlock
    //             it.second.pCache->setStatus(cacheStruct::INVALID);
    //             it.second.pCache->unlock();
    //         }
    //         it.second.pCache->release();
    //     }
    //     m_commitCache.clear();
    // }

    
}

int CBPlusTree::commit() {
    int lastIndicator = 0;
    int rc = 0;

    if (m_commitCache.empty()) {
        return 0;
    }

    // remove deleted node
    for(auto it = m_commitCache.begin(); it != m_commitCache.end(); ) {
        if (it->second.needDelete) {

            nodeLocker nl(it->second.pCache, m_page, it->second, it->second.nodeData->hdr->leaf, emptyCmpTypes);

            rc = nl.lock();
            if (rc == 0) {
                // lock success
                it->second.pCache->setStatus(cacheStruct::INVALID);
                nl.unlock();
            }
            // if lock fail, force to release the node, and let next operation to handle the dirty node
            rc = free_node(it->first, it->second.nodeData->hdr->leaf);
            if (rc != 0) {
                throw std::runtime_error("free_node failed during commit");
            }
            it->second.pCache->release();
            it = m_commitCache.erase(it);
        } else {
            ++it;
        }

    }

    if (m_commitCache.empty()) {
        return 0;
    }

    // commit changed node.
    size_t sz = m_commitCache.size();
    for(auto& node : m_commitCache) {

        if (B_END) {
            // TODO convert back to little endian if needed

            // before storing, convert back to little endian
            // node.second.nodeData->hdr->isConverted = 0;
        }

        #ifdef __BPDEBUG__
        cout << "Committing node gid: " << node.first.gid << " bid: " << node.first.bid << endl;
        cout << "pc addr: " << node.second.pCache << endl;
        #endif

        if (sz <= 1) {            
            rc = m_page.writeBack(node.second.pCache, &lastIndicator);
        } else {
            rc = m_page.writeBack(node.second.pCache, nullptr);
        }
        if (rc != 0) {
            return rc;
        }
        --sz;
    }

    while(lastIndicator != 1) {
        // wait for last write back complete
        if(lastIndicator == -1) {
            return -EIO;
        }
        std::this_thread::yield();
    }
    m_commitCache.clear();
    return 0;
}

// not support
int CBPlusTree::rollback() {

    // TODO

    return 0;

    // CSpinGuard g(m_lock);
    // if (m_commitCache.size() > 0) {
    //     // rollback uncommitted nodes
    //     for(auto& it : m_commitCache) {
    //         #ifdef __BPDEBUG__
    //         cout << "Rolling back node gid: " << it.first.gid << " bid: " << it.first.bid << endl;
    //         #endif

    //         if(it.second.newNode) {
    //             // new node, free disk space
    //             int rc = free_node(it.first, it.second.nodeData->hdr->leaf);
    //             if (rc != 0) {
    //                 return rc;
    //             }
    //         }

    //         int rc = it.second.pCache->lock();
    //         if (rc != 0) {
    //             return rc;
    //         }
    //         it.second.pCache->setStatus(cacheStruct::INVALID);
    //         it.second.pCache->unlock();
    //         it.second.pCache->release();
    //     }
    //     m_commitCache.clear();
    // }
    return 0;
}

int CBPlusTree::ensure_root() {
    int rc = 0;
    // if root exists, return
    if (m_root->bid != 0) return 0;

    NodeData root(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
    root.initNode(true, m_rowOrder, cmpTyps);
    root.self = allocate_node(true);
    if (root.self.bid == 0) return -ENOSPC;
    root.nodeData->hdr->childIsLeaf = 0;

    rc = store_node(root);
    if (rc != 0) {
        m_diskman.bfree(root.self.bid, m_rowPageSize);
        return rc;
    }

    *m_root = root.self;
    *high = 1;
    *m_begin = root.self;
    *m_end = {0, 0};
    return 0;
}

/*
    @param key key to insert
    @param row row buffer pointer
    @param len length of the input value
    @return 0 on success, else on failure
*/
int CBPlusTree::insert(const KEY_T& key, const void* row, uint32_t len) {
    CSpinGuard g(m_lock);
    // check if root exists, if not exists, create first leaf node
    int rc = ensure_root();
    if (rc != 0) return rc;

    char tempData[MAXKEYLEN];
    KEY_T upKey(tempData, key.len, cmpTyps);
    bidx upChild{0, 0};
    // if split is required, rc == SPLIT

    // search in root first
    rc = insert_recursive(*m_root, key, row, len, upKey, upChild, *high == 1 ? true : false);
    if (rc < 0) return rc;
    if (rc == SPLIT) {


        // root is leaf >> 
        if (*high == 1) {
            // create new root node, old root become left child, upChild become right child
            NodeData newRoot(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
            rc = newRoot.initNode(false, m_indexOrder, cmpTyps);
            if (rc != 0) {
                return rc;
            }
            newRoot.self = allocate_node(false);
            if (newRoot.self.bid == 0) return -ENOSPC;

            newRoot.nodeData->hdr->childIsLeaf = 1;

            // insert upKey and children
            rc = newRoot.insertChild(upKey, (*m_root).bid, upChild.bid);
            if (rc != 0) {
                return rc;
            }

            *m_root = newRoot.self;
            rc = store_node(newRoot);
            if (rc != 0) return rc;
            

            ++(*high);
            return 0;
        }

        // root is index node
        // split root node
        NodeData newRoot(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
        
        // right.initNode(false, m_indexOrder);
        newRoot.initNode(false, m_indexOrder, cmpTyps);

        newRoot.self = allocate_node(false);
        if (newRoot.self.bid == 0) return -ENOSPC;

        rc = newRoot.insertChild(upKey, (*m_root).bid, upChild.bid);
        if (rc != 0) return rc;

        *m_root = newRoot.self;

        rc = store_node(newRoot);
        if (rc != 0) return rc;
        
        ++(*high);
        return 0;
    }
    return 0;
}

int32_t CBPlusTree::insert_recursive(const bidx& idx, const KEY_T& key, const void* val, size_t valLen, KEY_T& upKey, bidx& upChild, bool isLeaf) {
    NodeData node(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
    int32_t rc = load_node(idx, node, isLeaf);
    if (rc != 0) return rc;

    // leaf node founded, inster the data
    if (node.nodeData->hdr->leaf) {
        // TODO:: IMPLEMENT LOWER_BOUND METHOD FOR KEY COMPARISON
        // KEY_T* it = std::lower_bound(node.keys, node.keys + node.nodeData->hdr.count + 1, key);

        // int pos = node.keyVec->search(key); // check if key already exists

        nodeLocker nl(node.pCache, m_page, node, isLeaf, cmpTyps);

        rc = nl.lock();
        if (rc != 0) {
            return rc;
        }

        rc = node.insertRow(key, val, valLen);
        if (rc) {
            nl.unlock();
            return rc;
        }

        if (node.keyVec->size() < m_rowOrder) {
            nl.unlock();
            store_node(node);
            return rc;
        }

        NodeData right(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
        right.initNode(true, m_rowOrder, cmpTyps);

        // upKey is used to return up key to parent
        split_leaf(node, right, upKey);
        nl.unlock();
        // update upchild object, at outside it will be used to update parent node
        upChild = right.self;
        store_node(node);
        store_node(right);

        return SPLIT;
    }
    // if not leaf, go to child node
    
    // >= key => right child, < key => left child
    int32_t idxChild = child_index(node, key);
    if (idxChild == -ERANGE) {
        idxChild = 0;
    }
    char tempData[MAXKEYLEN];
    KEY_T childUp{tempData, key.len, cmpTyps};
    bidx childNew{nodeId, 0};



    rc = insert_recursive({nodeId, node.childVec->at(idxChild)}, key, val, valLen, childUp, childNew, node.nodeData->hdr->childIsLeaf);
    if (rc < 0) return rc;
    if (rc == SPLIT) {
        // child node is split, need insert up key and new child pointer to current node


        /*

          ／＞\\\\フ
         |    _  _l
        ／` ミ＿꒳ノ
       /         |
      /   ヽ     ﾉ
      │    |  |  |
  ／￣|    |  |  |
  | (￣ヽ＿_ヽ_)__)
  ＼二つ    

                [2,5,15,24,37]
                    |
                   [5,8,9,10,12] -> [5,8,9,10,12,14]

                upkey = 10
                upchild = [10,12,14]
                oldchild = [5,8,9]

                (UK)
                    |
            [2,5,10,15,24,37]
                |  |
                |  [10,12,14]->(upchild)
                |
            [5,8,9] 
                                                                        
            [2,5,10,15,24,37] ====> [2,5,10]  [15,24,37](upchild) [15]->upkey

                        [15]
                     /       \
                    /         \        
                   /           \
                  /             \
            [2,5,10]             [15,24,37]
                |  |             |  
                |  |             |
                |  [10,12,14]    null
                [5,8,9]

        */
        nodeLocker nl(node.pCache, m_page, node, isLeaf, cmpTyps);
        CTemplateGuard g(nl);
        if (g.returnCode() != 0) {
            return g.returnCode();
        }


        // left child not change, only insert key and right child
        rc = node.insertChild(childUp, 0, childNew.bid);
        if (rc != 0) return rc;
        
        if (node.keyVec->size() < m_indexOrder) {
            return store_node(node);
        }
        // need split
        NodeData right(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);

        rc = split_internal(node, right, upKey);
        if (rc != 0) {
            return rc;
        }

        // use upChild to return new created right node index
        upChild = right.self;
        store_node(node);
        store_node(right);
        return SPLIT;
    }
    return store_node(node);
}

int CBPlusTree::search(const KEY_T& key, void* out, uint32_t len, uint32_t* actualLen) {
    CSpinGuard g(m_lock);
    if ((*m_root).bid == 0) return -ENOENT;
    bidx cur = *m_root;
    NodeData node(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
    bool nextIsLeaf = *high == 1 ? true : false;
    int rc = 0;
    while (true) {
        rc = node.deInitNode();
        if (rc != 0) return rc;
        
        rc = load_node(cur, node, nextIsLeaf);
        if (rc != 0) return rc;

        nodeLocker nl(node.pCache, m_page, node, nextIsLeaf, cmpTyps);
        CTemplateReadGuard g(nl);
        if (g.returnCode() != 0) {
            return g.returnCode();
        }

        if (node.nodeData->hdr->leaf) {
            int pos = node.keyVec->search(key);

            KEY_T foundKey(cmpTyps);
            rc = node.keyVec->at(pos, foundKey);
            if (!(rc == 0 && key == foundKey)) {
                return -ENOENT;
            }

            return node.rowVec->at(pos, reinterpret_cast<uint8_t*>(out), len, actualLen);
        }

        if (node.nodeData->hdr->childIsLeaf) {
            nextIsLeaf = true;
        }

        size_t idx = child_index(node, key);
        cur.bid = node.childVec->at(idx);
    }

    return 0;
}

CBPlusTree::iterator CBPlusTree::search(const KEY_T& key) {
    iterator it(*this, *m_root);
    
    CSpinGuard g(m_lock);
    if ((*m_root).bid == 0) {
        throw std::out_of_range("tree is empty");
    }
    bidx cur = *m_root;
    NodeData node(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
    bool nextIsLeaf = *high == 1 ? true : false;
    int rc = 0;
    while (true) {
        rc = node.deInitNode();
        if (rc != 0) {
            it.valid = false;
            return it;
        }

        rc = load_node(cur, node, nextIsLeaf);
        if (rc != 0) {
            it.valid = false;
            return it;
        }

        nodeLocker nl(node.pCache, m_page, node, nextIsLeaf, cmpTyps);
        CTemplateReadGuard g(nl);
        if (g.returnCode() != 0) {
            it.valid = false;
            return it;
        }
        if (node.nodeData->hdr->leaf) {
            int pos = node.keyVec->search(key);


            // KEY_T foundKey;
            // rc = node.keyVec->at(pos, foundKey);
            // if (!(rc == 0 && key == foundKey)) {
            //     // key not found 
            //     it.valid = false;
            //     return it;
            // }

            // return the closest key greater than or equal to the search key
            it.m_currentNode = cur;
            it.m_currentPos = pos;
            return it;            
        }

        if (node.nodeData->hdr->childIsLeaf) {
            nextIsLeaf = true;
        }

        size_t idx = child_index(node, key);
        cur.bid = node.childVec->at(idx);
    }

return it;
}

// TODO :: TEST THIS FUNCTION
int CBPlusTree::update(const KEY_T& key, const void* input, uint32_t len) {
    CSpinGuard g(m_lock);
    if ((*m_root).bid == 0) return -ENOENT;
    bidx cur = *m_root;
    NodeData node(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
    bool nextIsLeaf = *high == 1 ? true : false;
    int rc = 0;
    while (true) {
        rc = node.deInitNode();
        if (rc != 0) return rc;
        
        rc = load_node(cur, node, nextIsLeaf);
        if (rc != 0) return rc;

        nodeLocker nl(node.pCache, m_page, node, nextIsLeaf, cmpTyps);
        CTemplateReadGuard g(nl);
        if (g.returnCode() != 0) {
            return g.returnCode();
        }

        if (node.nodeData->hdr->leaf) {
            int pos = node.keyVec->search(key);

            KEY_T foundKey(cmpTyps);
            rc = node.keyVec->at(pos, foundKey);
            if (!(rc == 0 && key == foundKey)) {
                return -ENOENT;
            }
            
            uint8_t* rdata = nullptr;
            uint32_t actualLen = 0;
            // TODO :: check if primarykey is change?
            rc = node.rowVec->reference_at(pos, rdata, &actualLen);
            if (rc != 0) {
                return rc;
            }

            if (len > actualLen) {
                return -E2BIG;
            }

            std::memcpy(rdata, input, len);

            return 0;
        }

        if (node.nodeData->hdr->childIsLeaf) {
            nextIsLeaf = true;
        }

        size_t idx = child_index(node, key);
        cur.bid = node.childVec->at(idx);
    }

    return 0;
}

int CBPlusTree::remove(const KEY_T& key) {
    CSpinGuard g(m_lock);
    if ((*m_root).bid == 0) return -ENOENT;

    int rc = 0;   
    char tempData[MAXKEYLEN]; 
    KEY_T upKey{tempData, key.len, cmpTyps};
    bidx upChild {0, 0};
    // if split is required, rc == SPLIT

    // search in root first
    rc = remove_recursive(*m_root, key, upKey, *high == 1 ? true : false);
    if (rc < 0) return rc;
    if (rc == COMBINE) {
        // root need to be combined
        // if root is leaf, do nothing
        if (*high == 1) {
            return 0;
        }
    } else if (rc == EMPTY) {
        // tree become empty
        *m_root = {0, 0};
        *high = 0;
    }
    
    return 0;
}

int32_t CBPlusTree::remove_recursive(const bidx& idx, const KEY_T& key, KEY_T& upKey, bool isLeaf) {
    CBPlusTree::NodeData node(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
    int32_t rc = load_node(idx, node, isLeaf);
    if (rc != 0) return rc;
    int updateType = 0;

    // leaf node founded, inster the data
    if (isLeaf) {

        nodeLocker nl(node.pCache, m_page, node, isLeaf, cmpTyps);

        CTemplateGuard g(nl);
        if (g.returnCode() != 0) {
            return g.returnCode();
        }

        // if first key is the key to delete, need to update upKey
        KEY_T foundKey(cmpTyps);
        rc = node.keyVec->at(0, foundKey);
        if (rc == 0 && key == foundKey) {
            // key is first element, need to update parent key
            node.keyVec->pop_front();
            node.rowVec->pop_front();
            if (node.keyVec->size() > 0) {
                upKey = (*node.keyVec)[0];
                updateType |= UPDATEKEY;
            }
        } else {
            // normal delete
            rc = node.erase(key);
            if (rc != 0) return rc;
        }

        
        // check if need to combine
        if (node.size() < (m_rowOrder / 2) && node.self.bid != (*m_root).bid) {
            // need to combine or borrow from sibling
            rc = store_node(node);
            if (rc != 0) return rc;
            updateType |= COMBINE;
            return updateType;
        }
        rc = store_node(node);
        if (rc != 0) return rc;

        return updateType;
    }

    char tempData[MAXKEYLEN];
    KEY_T childUp{tempData, key.len, cmpTyps};
    bidx childNew{nodeId, 0};

    nodeLocker nl(node.pCache, m_page, node, isLeaf, cmpTyps);

    rc = nl.read_lock();
    if (rc != 0) return rc;

    int32_t idxChild = child_index(node, key);

    if (idxChild == -ERANGE) {
        // key not found, no need to delete
        nl.read_unlock();
        return 0;
    }
    bidx nextNode = {nodeId, node.childVec->at(idxChild)};
    bool nextIsLeaf = node.nodeData->hdr->childIsLeaf ? true : false;
    nl.read_unlock();

    rc = remove_recursive(nextNode, key, childUp, nextIsLeaf);
    if (rc < 0) {
        return rc;
    }

    if (rc & UPDATEKEY) {
        // need to update the index key
        nodeLocker nl(node.pCache, m_page, node, isLeaf, cmpTyps);

        CTemplateGuard g(nl);
        if (g.returnCode() != 0) {
            return g.returnCode();
        }
        uint32_t pos = 0;
        pos = node.keyVec->search(key);
        if (pos >= node.keyVec->size()) {
          
        } else if (key == (*node.keyVec)[pos]) {
            // update key, and pass the key to parent
            (*node.keyVec)[pos] = childUp;
        }

        updateType |= UPDATEKEY;
        upKey = childUp;

    } 

    if (rc & COMBINE) {
        // need combine two sibling child nodes, or borrow from sibling
        nodeLocker nl(node.pCache, m_page, node, isLeaf, cmpTyps);
        CTemplateGuard g(nl);
        if (g.returnCode() != 0) {
            return g.returnCode();
        }

        // check wether need to combine child node or borrow from sibling
        rc = combine_child(node, idxChild);
        if (rc != 0) return rc;

        // check if need to combine current node
        if (node.size() < m_indexOrder / 2 && node.self.bid != (*m_root).bid) {
            updateType |= COMBINE;
        }
    } 
    
    rc = store_node(node);
    if (rc != 0) return rc;
    

    return updateType;
}

int CBPlusTree::destroy() {
    if (m_root->bid == 0) {
        return 0;
    }
    int rc = 0;
    bool isLeaf = *high == 1 ? true : false;
    rc = destroy_recursive(*m_root, isLeaf);
    if (rc != 0) return rc;
    *m_root = {0, 0};
    *m_begin = {0, 0};
    *m_end = {0, 0};
    *high = 0;
    return 0;
}

int CBPlusTree::destroy_recursive(const bidx& idx, bool isLeaf) {
    NodeData node(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
    int rc = load_node(idx, node, isLeaf);
    if (rc != 0) return rc;

    nodeLocker nl(node.pCache, m_page, node, isLeaf, cmpTyps);
    CTemplateGuard g(nl);
    if (g.returnCode() != 0) {
        return g.returnCode();
    }

    if (!node.nodeData->hdr->leaf) {
        // internal node, need destroy child nodes first
        for (uint32_t i = 0; i < node.childVec->size(); ++i) {
            bidx childIdx{nodeId, node.childVec->at(i)};
            rc = destroy_recursive(childIdx, node.nodeData->hdr->childIsLeaf);
            if (rc != 0) return rc;
        }
    }

    m_commitCache.emplace(idx, std::move(node));

    // free current node
    rc = free_node(idx, isLeaf);
    if (rc != 0)  { 
        #ifdef __BPDEBUG__
        cout << "Failed to free node gid: " << idx.gid << " bid: " << idx.bid << endl;
        abort();
        #endif
        return rc;
    }
    

    node.pCache->release();
    return 0;
}

int32_t CBPlusTree::combine_child(NodeData& node, int32_t idxChild) {

    int leftIdx = idxChild == 0 ? -1 : idxChild - 1;
    int rightIdx = static_cast<uint32_t>(idxChild) == node.keyVec->size() ?  -1 : idxChild + 1;

    enum combineType : uint8_t {
        ERROR,
        BORROW,
        MERGE  
    };

    combineType ct = ERROR;
    bool isLeaf = node.nodeData->hdr->childIsLeaf ? true : false;
    bool fromLeft = false;
    NodeData* borrowNode = nullptr;
    NodeData left(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen), 
             right(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen), 
             target(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);

    nodeLocker  leftLocker(node.pCache, m_page, node, isLeaf, cmpTyps),
                rightLocker(node.pCache, m_page, node, isLeaf, cmpTyps),
                targetLocker(node.pCache, m_page, node, isLeaf, cmpTyps);


    int rc = 0;
    // load left child
    bidx idx = {nodeId, 0}; 

    // load target child node
    rc = load_node({nodeId, node.childVec->at(idxChild)}, target, isLeaf);
    if (rc != 0) return rc;

    // select sibling to borrow or combine
    if (leftIdx != -1) {
        idx.bid = node.childVec->at(leftIdx);
        rc = load_node(idx, left, isLeaf);
        if (rc != 0) return rc;
    }
    
    if (rightIdx != -1) {
        // load right child
        idx.bid = node.childVec->at(rightIdx);
        rc = load_node(idx, right, isLeaf);
        if (rc != 0) return rc;
    }

    if (!left.inited && !right.inited) {
        return -EFAULT;
    } else if (left.inited && !right.inited) {
        borrowNode = &left;
        fromLeft = true;
    } else if (!left.inited && right.inited){
        borrowNode = &right;
        fromLeft = false;
    } else {
        rc = leftLocker.read_lock();
        if (rc != 0) return rc;
        rc = rightLocker.read_lock();
        if (rc != 0) {
            leftLocker.read_unlock();
            return rc;
        }
        
        // both sibling exists, choose larger one
        if (left.size() >= right.size()) {
            borrowNode = &left;
            fromLeft = true;
        } else {
            borrowNode = &right;
            fromLeft = false;
        }
        leftLocker.read_unlock();
        rightLocker.read_unlock();
    }

    // lock two nodes
    nodeLocker target_nl(target.pCache, m_page, node, isLeaf, cmpTyps);
    nodeLocker borrow_nl(borrowNode->pCache, m_page, node, isLeaf, cmpTyps);

    CTemplateGuard gt(target_nl);
    if (gt.returnCode() != 0) {
        return gt.returnCode();
    }
    // lock two nodes
    CTemplateGuard gb(borrow_nl);
    if (gb.returnCode() != 0) {
        return gb.returnCode();
    }

    // if sibling has enough keys, borrow from sibling, else merge two nodes
    if (borrowNode->size() > (isLeaf ? (m_rowOrder / 2) : (m_indexOrder / 2))) {
        ct = BORROW;
        #ifdef __BPDEBUG__
        // std::cout << "BORROW from " << (fromLeft ? "LEFT" : "RIGHT") << " sibling node." << std::endl;
        // std::cout << "  Target Node before borrow: ";
        // target.printNode();
        // std::cout << "  Borrow Node before borrow: ";
        // borrowNode->printNode();
        #endif
    } else {
        ct = MERGE;
    }

    if (ct == BORROW) {
        // borrow from sibling node
        rc = borrowKey(node, *borrowNode, target, isLeaf, fromLeft, idxChild);
        if (rc != 0) return rc;
    } else if (ct == MERGE) {
        // merge two nodes
        rc = mergeNode(node, *borrowNode, target, isLeaf, fromLeft, idxChild);
        if (rc != 0) return rc;

    } else {
        return -EFAULT;
    }


    
    return 0;

}

int CBPlusTree::borrowKey(NodeData& parent, NodeData& fromNode, NodeData& toNode, bool isLeaf, bool fromLeft, int targetIdx) {


    /*
BORROW KEY FROM SIBLING NODE
                          parent
                            |
                       from | target    
                          | | |
                    [   1   7   15   30   50   ]
                         /    \
                  [ 1 3 5]   [ 7 ]

LEFT
                    [  1  5  15  30  50  ]
                        /   \
                  [ 1 3 ]   [ 5  7 ]

RIGHT
                          parent
                            |
                       from | target    
                          | | |
                    [   1   3   15   30   50   ]
                         /    \
                    [ 1 ]   [ 3 5 7 ]

                    [  1  5  15  30  50  ]
                        /   \
                  [ 1 3 ]   [ 5  7 ]

    */

    // parent change key index
    int parentKeyIdx = fromLeft ? targetIdx - 1 : targetIdx;
    // child borrow key index
    int fromKeyIdx = fromLeft ? fromNode.keyVec->size() - 1 : 0;
    
    
    if (isLeaf) {
        // borrow key from sibling leaf node

        // move key and row data from fromNode to toNode
        KEY_T fromKey(cmpTyps);
        int rc = fromNode.keyVec->at(fromKeyIdx, fromKey);
        if (rc != 0) return rc;

        KEY_T parentKey(cmpTyps);
        rc = parent.keyVec->at(parentKeyIdx, parentKey);
        if (rc != 0) return rc;
        
        uint8_t* rowData = nullptr;
        uint32_t rowLen = 0;
        // copy borrow data to target
        rc = fromNode.rowVec->reference_at(fromKeyIdx, rowData, &rowLen);
        if (rc != 0) return rc;


        // change parent key
        if (fromLeft) {
            rc = toNode.pushFrontRow(fromKey, rowData, rowLen);
            if (rc != 0) return rc;
            // left borrow use last key
            parentKey = fromKey;
            fromNode.popBackRow();
        } else {
            // may cause 2 parent key change
            
            rc = toNode.pushBackRow(fromKey, rowData, rowLen);
            if (rc != 0) return rc;
            // right borrow use second key
            parentKey = *(fromNode.keyVec->begin() + (fromKeyIdx + 1));
            // *parentKey = *(fromKey + 1);
            // cout << "frontkeyidx = " << fromKeyIdx << endl;
            // cout << "parentKeyIdx = " << parentKeyIdx << endl;
            // fromNode.printNode();
            fromNode.popFrontRow();
        }

    } else {
        // borrow key from sibling index
        // change index node
        // move key and child from fromNode to toNode
        KEY_T fromKey(cmpTyps);
        // get key from sibling node
        int rc = fromNode.keyVec->at(fromKeyIdx, fromKey);
        if (rc != 0) return rc;

        KEY_T parentKey(cmpTyps);
        // get change key from parent node
        rc = parent.keyVec->at(parentKeyIdx, parentKey);
        if (rc != 0) return rc;
        
        char tempData[MAXKEYLEN];
        KEY_T targetKey{tempData, parentKey.len, cmpTyps};
        targetKey = parentKey;
        
        // get target insert key from target node

        // rc = toNode.keyVec->at(fromLeft ? 0 : toNode.keyVec->size() - 1, targetKey);
        // if (rc != 0) return rc;
        // *targetKey = *parentKey;

        parentKey = fromKey;

        child_t fromNodeChild = 0;
        // if from left, get last child, else get first child
        fromNodeChild = fromNode.childVec->at(fromLeft ? fromNode.size() : 0);
        if (rc != 0) return rc;

        // insert child into target node
        if (fromLeft) {
            // insert to left
            rc = toNode.pushFrontChild({targetKey.data, targetKey.len, cmpTyps}, fromNodeChild);
            if (rc != 0) return rc;
            fromNode.popBackChild();
        } else {

            // insert to right
            rc = toNode.pushBackChild({targetKey.data, targetKey.len, cmpTyps}, fromNodeChild);
            if (rc != 0) return rc;
            fromNode.popFrontChild();
        }
    }
    return 0;
}

int CBPlusTree::mergeNode(NodeData& parent, NodeData& fromNode, NodeData& toNode, bool isLeaf, bool fromLeft, int targetIdx) {

    KEY_T parentKey(cmpTyps);
    int rc = 0;
    
    NodeData* rightNode = &toNode;
    NodeData* leftNode = &fromNode;
    int parentKeyIdx = targetIdx - 1;

    if (!fromLeft) {
        rightNode = &fromNode;
        leftNode = &toNode;
        parentKeyIdx = targetIdx;
    }

    rc = parent.keyVec->at(parentKeyIdx, parentKey);
    if (rc != 0) return rc;


    if (isLeaf) {

        // delete the right node, move right node data to left node
        rc = leftNode->pushBackRows(*rightNode);
        if (rc != 0) return rc;
        // will also delete right child of the parent key
        parent.erase(parentKeyIdx, parentKeyIdx + 1);
        if (rc != 0) return rc;

        // delete right child node
        rightNode->needDelete = true;
        store_node(*rightNode);
    

        return 0;

    }

    /*
    example:

                   parent
                     |
                left | right    
                   | | |
            [  1  5  8  13  16  20  27 ]
                   /   \
          [ 5 6 7 ]    [ 9 11 ]
                 |      / \
                [7]   [8] [9 10]
   



            [  1  5  13  16  20  27]
                   /  
          [ 5 6 7 8 9 11 ]

    */

    // move the parent's key to left node, and merge right node childs to left node

    // [ 5 6 7 ]    [ 9 11 ] => [ 5 6 7 8 9 11 ]
    rc = leftNode->pushBackChilds(*rightNode, parentKey);
    if (rc != 0) return rc;

    //  [  1  5  8  13  16  20  27 ] => [  1  5  13  16  20  27]
    rc = parent.erase(parentKeyIdx, parentKeyIdx + 1);
    if (rc != 0) return rc;

    // delete right child node
    rightNode->needDelete = true;
    store_node(*rightNode);

    if (parent.size() == 0 && parent.self.bid == (*m_root).bid) {
        // root node become empty, update m_root
        *m_root = leftNode->self;
        --(*high);
    }



    return 0;
}

#ifdef __BPDEBUG__


void CBPlusTree::printTree() {
    CSpinGuard g(m_lock);
    if ((*m_root).bid == 0) {
        std::cout << "B+ Tree is empty." << std::endl;
        return;
    }
    std::cout << "B+ Tree Structure (Height: " << (uint32_t)(*high) << "):(Begin: " << (*m_begin).bid << ")" << std::endl;
    printTreeRecursive(*m_root, *high == 1 ? true : false, *high);

}

const char hex_chars[] = "0123456789ABCDEF";

void CBPlusTree::printTreeRecursive(const bidx& idx, bool isLeaf, int level) {
    NodeData node(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);


    int rc = load_node(idx, node, isLeaf);
    if (rc != 0) {
        std::cout << "Failed to load node at bid " << idx.bid << std::endl;
        return;
    }
    
    nodeLocker nl(node.pCache, m_page, node, isLeaf, cmpTyps);
    CTemplateReadGuard g(nl);
    if (g.returnCode() != 0) {
        std::cout << "Failed to read lock node at bid " << idx.bid << std::endl;
        return;
    }

    std::cout << "----------------------------------------" << std::endl;
    std::cout << "Level " << (*high - level + 1) << " - Node Bid: " << idx.bid << ", Keys: \n";
    std::cout << "node header : " << " leaf: " << (int)node.nodeData->hdr->leaf 
              << " childIsLeaf: " << (int)node.nodeData->hdr->childIsLeaf 
              << " key count: " << node.keyVec->size() << endl;
    if (node.nodeData->hdr->leaf) {
        cout << " prev leaf bid: " << node.nodeData->hdr->prev
              << " next leaf bid: " << node.nodeData->hdr->next << std::endl; 
    }
    for (size_t i = 0; i < node.keyVec->size(); ++i) {
        KEY_T key;
        rc = node.keyVec->at(i, key);
        if (rc == 0) {
            std::string keyStr;
            for(int i = 0; i < keyLen; ++i) {
                keyStr.push_back(hex_chars[((key).data[i] >> 4) & 0x0F]);
                keyStr.push_back(hex_chars[(key).data[i] & 0x0F]);
                keyStr.push_back(' ');
                if(i % 16 == 15 && i != keyLen - 1) {
                    keyStr.push_back('\n');
                }
            }

            std::cout << keyStr << " ";
        }

        // print left child if index node
        if (!node.nodeData->hdr->leaf) {
            std::cout << " | Child Bid: " << node.childVec->at(i) << endl;
        } else {
            char rowDataPreview[128] = {0};
            uint8_t* rowData = nullptr;
            uint32_t rowLen = 0;
            rc = node.rowVec->reference_at(i, rowData, &rowLen);
            if (rc == 0) {
                size_t previewLen = rowLen < 32 ? rowLen : 32;
                for (size_t j = 0; j < previewLen; ++j) {
                    rowDataPreview[j * 2] = hex_chars[(rowData[j] >> 4) & 0x0F];
                    rowDataPreview[j * 2 + 1] = hex_chars[rowData[j] & 0x0F];
                }
                rowDataPreview[previewLen * 2] = '\0';
                std::cout << " | Row Data (len " << rowLen << "): " << rowDataPreview << endl;
            }
        }
    }
    
    if (!node.nodeData->hdr->leaf) {
        std::cout << "last child bid: " << node.childVec->at(node.keyVec->size()) << std::endl;
        std::cout << std::endl;
        for (size_t i = 0; i <= node.keyVec->size(); ++i) {
            bidx childIdx = {nodeId, node.childVec->at(i)};
            printTreeRecursive(childIdx, node.nodeData->hdr->childIsLeaf, level - 1);
        }
    } else {
        std::cout << std::endl;
    }
}

#else 
void CBPlusTree::printTree() {
    // do nothing
}
void CBPlusTree::printTreeRecursive(const bidx& idx, bool isLeaf, int level)  {
    // do nothing
}
#endif


int CBPlusTree::NodeData::initNode(bool isLeaf, int32_t order, const std::vector<std::pair<uint8_t, dpfs_datatype_t>>& keyType) {
    if (inited) {
        return -EALREADY;
    }
    int rc = 0;
    void* zptr = isLeaf ? m_page.alloczptr(m_rowPageSize) : m_page.alloczptr(m_pageSize);
    if (zptr == nullptr) {
        rc = -ENOMEM;
        goto errReturn;
    }

    nodeData = new nd(isLeaf, zptr, m_pageSize, m_rowPageSize);
    if (nodeData == nullptr) {
        rc = -ENOMEM;
        goto errReturn;
    }
    memset(nodeData->data, 0, isLeaf ? m_rowPageSize : m_pageSize);
    nodeData->hdr->leaf = isLeaf ? 1 : 0;
    
    maxKeySize = order;
    keys = (reinterpret_cast<uint8_t*>(nodeData->data + sizeof(NodeData::nd::hdr_t)));
    keyVec = new CKeyVec<KEY_T, uint16_t>(keys, nodeData->hdr->count, keyLen, maxKeySize + 1, keyType);
    if (keyVec == nullptr) {
        rc = -ENOMEM;
        goto errReturn;
    }

    if (!isLeaf) {
        // pass header and keys area
        // constexpr uint32_t order32 = (PAGESIZE * 4096 - sizeof(NodeData::nd::hdr_t) - sizeof(uint64_t)) / (KEYLEN + sizeof(uint64_t));
        children = (reinterpret_cast<uint64_t*>(nodeData->data + sizeof(NodeData::nd::hdr_t) + (keyLen * order)));
        childVec = new CChildVec(*this);
        if (childVec == nullptr) {
            rc = -ENOMEM;
            goto errReturn;
        }
    } else {

        // values = (reinterpret_cast<void*>(nodeData->data + sizeof(nodeData->hdr) + (KEYLEN * order)));
        // values = reinterpret_cast<void*>(keys);
        rowVec = new CRowVec(*this, m_rowLen);
        if (rowVec == nullptr) {
            rc = -ENOMEM;
            goto errReturn;
        }
    }

    inited = true;
    return 0;
errReturn:
    if (nodeData) {
        delete nodeData;
        nodeData = nullptr;
    }
    if (keyVec) {
        delete keyVec;
        keyVec = nullptr;
    }
    if (childVec) {
        delete childVec;
        childVec = nullptr;
    }
    if (rowVec) {
        delete rowVec;
        rowVec = nullptr;
    }
    if (zptr) {
        m_page.freezptr(zptr, isLeaf ? m_rowPageSize : m_pageSize);
        zptr = nullptr;
    }

    return rc;
}

int CBPlusTree::NodeData::initNodeByLoad(bool isLeaf, int32_t order, void* zptr, const std::vector<std::pair<uint8_t, dpfs_datatype_t>>& keyType) {
    if (inited) {
        return -EALREADY;
    }
    int rc = 0;
    nodeData = new nd(isLeaf, zptr, m_pageSize, m_rowPageSize);
    if (nodeData == nullptr) {
        rc = -ENOMEM;
        goto errReturn;
    }

    maxKeySize = order;
    keys = (reinterpret_cast<uint8_t*>(nodeData->data + sizeof(NodeData::nd::hdr_t)));
    keyVec = new CKeyVec<KEY_T, uint16_t>(keys, nodeData->hdr->count, keyLen, maxKeySize + 1, keyType);
    if (keyVec == nullptr) {
        rc = -ENOMEM;
        goto errReturn;
    }

    if (!isLeaf) {
        // pass header and keys area
        children = (reinterpret_cast<uint64_t*>(nodeData->data + sizeof(NodeData::nd::hdr_t) + (keyLen * order)));
        childVec = new CChildVec(*this);
        if (childVec == nullptr) {
            rc = -ENOMEM;
            goto errReturn;
        }
    } else {
        // values = (reinterpret_cast<void*>(nodeData->data + sizeof(nodeData->hdr) + (KEYLEN * order)));
        // values = reinterpret_cast<void*>(keys);
        rowVec = new CRowVec(*this, m_rowLen);
        if (rowVec == nullptr) {
            rc = -ENOMEM;
            goto errReturn;
        }
    }

    inited = true;
    return 0;
errReturn:
    if (nodeData) {
        delete nodeData;
        nodeData = nullptr;
    }
    if (keyVec) {
        delete keyVec;
        keyVec = nullptr;
    }
    if (childVec) {
        delete childVec;
        childVec = nullptr;
    }
    if (rowVec) {
        delete rowVec;
        rowVec = nullptr;
    }

    return rc;
}

int CBPlusTree::NodeData::deInitNode() {
    if (!inited) {
        return 0;
    }

    if (isRef) {
        this->self          = {0, 0};
        this->keys          = nullptr;
        this->children      = nullptr;
        this->keyVec        = nullptr;
        this->childVec      = nullptr;
        this->rowVec        = nullptr;
        this->nodeData      = nullptr;
        this->maxKeySize    = 0;
        this->pCache        = nullptr;
        this->inited        = false;
        this->isRef         = false;
        return 0;
    }

    if (rowVec) {
        delete rowVec;
        rowVec = nullptr;
    }
    if (childVec) {
        delete childVec;
        childVec = nullptr;
    }
    if (keyVec) {
        delete keyVec;
        keyVec = nullptr;
    }
    if (nodeData) {
        if (!pCache && nodeData->data) {
            m_page.freezptr(nodeData->data, nodeData->hdr->leaf ? m_rowPageSize : m_pageSize);
            nodeData->data = nullptr;
        }
        delete nodeData;
        nodeData = nullptr;
    }

    if (pCache) {
        pCache->release();
        pCache = nullptr;
    }
    inited = false;
    return 0;
}

int CBPlusTree::NodeData::insertChild(const KEY_T& key, uint64_t lchild, uint64_t rchild) {
    if (nodeData->hdr->leaf) {
        // is leaf node insert child is invalid
        return -EINVAL;
    }
    uint32_t pos = 0;
    int rc = keyVec->insert(key);
    if (rc < 0) {
        return rc;
    }
    pos = rc;
    // insert child pointer
    /*           0 1  2  3
                [5,15,24,37]
                0 1  2  3  4
                |
            [0,1,2,3,4]      ----->    [-1,0,1]  [2,3,4]


                 0 1 2  3  4
                [2,5,15,24,37]
                0 1 2  3  4  5
                | |
                | [2,3,4]
                |
            [-1,0,1]
    */
    if (lchild) {
        if (pos == childVec->size()) {
            // insert at the end
            childVec->push_back(lchild);
        } else if (pos < childVec->size()) {
            childVec->insert(pos, lchild);
        } else {
            return -EFAULT;
        }
        
        // childVec->at(pos) = lchild;

    }

    if (pos + 1 == childVec->size()) {
        // insert at the end
        childVec->push_back(rchild);
    } else if (pos + 1 < childVec->size()) {
        childVec->insert(pos + 1, rchild);
    } else {
        return -EFAULT;
    }

    return 0;

}

int CBPlusTree::NodeData::pushBackChild(const KEY_T& key, uint64_t child) {
    if (nodeData->hdr->leaf) {
        // is leaf node insert child is invalid
        return -EINVAL;
    
    }
    int rc = 0;
    // insert child pointer, at the end
    rc = childVec->push_back(child);
    if (rc < 0) {
        return rc;
    }
    rc = keyVec->push_back(key);
    if (rc < 0) {
        childVec->pop_back();
        return rc;
    }

    return 0;

}

int CBPlusTree::NodeData::pushBackChilds(const NodeData& fromNode, const KEY_T& concateKey) noexcept {
    if (nodeData->hdr->leaf || fromNode.nodeData->hdr->leaf) {
        return -EINVAL;
    }

    int rc = 0;

    rc = childVec->concate_back(*fromNode.childVec);
    if (rc < 0) {
        return rc;
    }

    // insert the concate key
    rc = keyVec->push_back(concateKey);
    if (rc < 0) return rc;

    rc = keyVec->concate_back(*fromNode.keyVec);
    if (rc < 0) {
        keyVec->pop_back();
        // if error occur, vecsize is not changed, so no need to rollback childVec
        return rc;
    }

    return 0;
}

int CBPlusTree::NodeData::pushBackRow(const KEY_T& key, const void* rowData, size_t dataLen) {
    if (!nodeData->hdr->leaf) {
        return -EINVAL;
    }

    int rc = 0;
    // insert row data at the end
    rc = rowVec->push_back(rowData, dataLen);
    if (rc < 0) {
        return rc;
    }
    rc = keyVec->push_back(key);
    if (rc < 0) {
        rowVec->pop_back();
        return rc;
    }
    return 0;

}

int CBPlusTree::NodeData::pushBackRows(const NodeData& fromNode) noexcept {

    if (!nodeData->hdr->leaf || !fromNode.nodeData->hdr->leaf) {
        return -EINVAL;
    }

    int rc = 0;

    rc = rowVec->concate_back(*fromNode.rowVec);
    if (rc < 0) {
        return rc;
    }
    rc = keyVec->concate_back(*fromNode.keyVec);;
    if (rc < 0) {
        // if error occur, vecsize is not changed, so no need to rollback rowVec
        return rc;
    }

    return 0;

}

int CBPlusTree::NodeData::pushFrontChild(const KEY_T& key, uint64_t child) {
    if (nodeData->hdr->leaf) {
        // is leaf node insert child is invalid
        return -EINVAL;
    }

    int rc = 0;
    // insert child pointer, at the front
    rc = childVec->push_front(child);
    if (rc < 0) {
        return rc;
    }
    rc = keyVec->push_front(key);
    if (rc < 0) {
        childVec->pop_front();
        return rc;
    }
    return 0;
}

int CBPlusTree::NodeData::pushFrontChilds(const NodeData& fromNode, const KEY_T& concateKey) noexcept {
    if (nodeData->hdr->leaf || fromNode.nodeData->hdr->leaf) {
        return -EINVAL;
    }

    int rc = 0;

    rc = childVec->concate_front(*fromNode.childVec);
    if (rc < 0) {
        return rc;
    }

    rc = keyVec->push_front(concateKey);
    if (rc < 0) return rc;
    
    rc = keyVec->concate_front(*fromNode.keyVec);
    if (rc < 0) {
        // if error occur, vecsize is not changed, so no need to rollback childVec
        keyVec->pop_front();
        return rc;
    }
    return 0;
}

int CBPlusTree::NodeData::pushFrontRow(const KEY_T& key, const void* rowData, size_t dataLen) {
    if (!nodeData->hdr->leaf) {
        return -EINVAL;
    }

    int rc = 0;
    // insert row data at the front
    rc = rowVec->push_front(rowData, dataLen);
    if (rc < 0) {
        return rc;
    }
    rc = keyVec->push_front(key);
    if (rc < 0) {
        rowVec->pop_front();
        return rc;
    }
    return 0;
}

int CBPlusTree::NodeData::pushFrontRows(const NodeData& fromNode) noexcept {
    if (!nodeData->hdr->leaf || !fromNode.nodeData->hdr->leaf) {
        return -EINVAL;
    }

    int rc = 0;

    rc = rowVec->concate_front(*fromNode.rowVec);
    if (rc < 0) {
        return rc;
    }
    rc = keyVec->concate_front(*fromNode.keyVec);;
    if (rc < 0) {
        // if error occur, vecsize is not changed, so no need to rollback rowVec
        return rc;
    }

    return 0;
}

int CBPlusTree::NodeData::popFrontRow() {
    if (!nodeData->hdr->leaf) {
        return -EINVAL;
    }

    int rc = 0;
    rowVec->pop_front();
    rc = keyVec->pop_front();
    if (rc < 0) {
        return rc;
    }
    return 0;
}

int CBPlusTree::NodeData::popBackRow() {
    if (!nodeData->hdr->leaf) {
        return -EINVAL;
    }

    int rc = 0;
    rowVec->pop_back();
    rc = keyVec->pop_back();
    if (rc < 0) {
        return rc;
    }
    return 0;
}

int CBPlusTree::NodeData::popFrontChild() {
    if (nodeData->hdr->leaf) {
        // is leaf node insert child is invalid
        return -EINVAL;
    }

    int rc = 0;
    childVec->pop_front();
    rc = keyVec->pop_front();
    if (rc < 0) {
        return rc;
    }
    return 0;
}

int CBPlusTree::NodeData::popBackChild() {
    if (nodeData->hdr->leaf) {
        // is leaf node insert child is invalid
        return -EINVAL;
    }

    int rc = 0;
    childVec->pop_back();
    rc = keyVec->pop_back();
    if (rc < 0) {
        return rc;
    }
    return 0;
}

int CBPlusTree::NodeData::insertRow(const KEY_T& key, const void* rowData, size_t dataLen) {
    if (!nodeData->hdr->leaf) {
        return -EINVAL;
    }

    int pos = 0;
    int rc = keyVec->insert(key);
    if (rc < 0) {
        return rc;
    }
    pos = rc;

    rc = rowVec->insert(pos, rowData, dataLen);
    if (rc < 0) {
        keyVec->erase(pos, pos + 1);
        return rc;
    }
    return 0;

}

CBPlusTree::NodeData::NodeData(NodeData&& nd) noexcept : m_page(nd.m_page) {
    #ifdef __BPDEBUG__
    std::cout << "NodeData move constructor called for bid: " << nd.self.bid << std::endl;
    #endif
    this->self = nd.self;
    this->keys = nd.keys;
    this->children = nd.children;
    this->keyVec = nd.keyVec;
    this->childVec = nd.childVec;
    this->rowVec = nd.rowVec;
    this->nodeData = nd.nodeData;
    this->maxKeySize = nd.maxKeySize;
    this->pCache = nd.pCache;
    this->inited = nd.inited;
    this->keyLen = nd.keyLen;

    nd.keys = nullptr;
    nd.children = nullptr;
    nd.keyVec = nullptr;
    nd.childVec = nullptr;
    nd.rowVec = nullptr;
    nd.nodeData = nullptr;
    nd.pCache = nullptr;
    nd.inited = false;

}

CBPlusTree::NodeData& CBPlusTree::NodeData::operator=(NodeData&& nd) {
    this->self = nd.self;
    this->keys = nd.keys;
    this->children = nd.children;
    this->keyVec = nd.keyVec;
    this->childVec = nd.childVec;
    this->rowVec = nd.rowVec;
    this->nodeData = nd.nodeData;
    this->maxKeySize = nd.maxKeySize;
    this->pCache = nd.pCache;
    this->inited = nd.inited;
    this->keyLen = nd.keyLen;

    nd.keys = nullptr;
    nd.children = nullptr;
    nd.keyVec = nullptr;
    nd.childVec = nullptr;
    nd.rowVec = nullptr;
    nd.nodeData = nullptr;
    nd.pCache = nullptr;
    nd.inited = false;
    return *this;
}

CBPlusTree::NodeData& CBPlusTree::NodeData::operator=(NodeData* nd) {
    this->self = nd->self;
    this->keys = nd->keys;
    this->children = nd->children;
    this->keyVec = nd->keyVec;
    this->childVec = nd->childVec;
    this->rowVec = nd->rowVec;
    this->nodeData = nd->nodeData;
    this->maxKeySize = nd->maxKeySize;
    this->pCache = nd->pCache;
    this->inited = nd->inited;
    this->keyLen = nd->keyLen;
    this->isRef = true;

    return *this;
}

CBPlusTree::NodeData::~NodeData() {
    if (isRef) {
        return;
    }
    #ifdef __BPDEBUG__
    std::cout << "NodeData destructor called for bid: " << self.bid << std::endl;
    #endif



    if (rowVec) {
        delete rowVec;
        rowVec = nullptr;
    }
    if (childVec) {
        delete childVec;
        childVec = nullptr;
    }
    if (keyVec) {
        delete keyVec;
        keyVec = nullptr;
    }
    if (nodeData) {
        if (!pCache && nodeData->data) {
            m_page.freezptr(nodeData->data, nodeData->hdr->leaf ? m_rowPageSize : m_pageSize);
            nodeData->data = nullptr;
        }
        delete nodeData;
        nodeData = nullptr;
    }

    if (pCache) {
        pCache->release();
        pCache = nullptr;
    }

}


int CBPlusTree::NodeData::size() noexcept {
    return nodeData->hdr->count;
}

int CBPlusTree::NodeData::assign(const NodeData& target, int begin, int end) noexcept {
    int rc = keyVec->assign(target.keyVec->begin() + begin, target.keyVec->begin() + end);
    if (rc < 0) {
        return rc;
    }
    if (!nodeData->hdr->leaf) {
        rc = childVec->assign(&target.children[begin], &target.children[end + 1]);
        if (rc < 0) {
            return rc;
        }
    } else {
        rc = rowVec->assign(&target.rowVec->row[begin * rowVec->m_rowLen], &target.rowVec->row[end * rowVec->m_rowLen]);
        if (rc < 0) {
            return rc;
        }
    }
    return 0;
}


/*
     1 2 4 5
    1 2 3 4 5

    erase 2~4

     1 5
    1 2 

    erase 0~2

     4 5
    / 4 5

    erase 3~5

     1 2
    1 2 /
*/

int CBPlusTree::NodeData::erase(int begin, int end) noexcept {
    // erase children or rows first
    int rc = 0;
    if (!nodeData->hdr->leaf) {
        // only for split, else may cause error
        rc = childVec->erase(begin + 1, end + 1);
        if (rc < 0) {
            return rc;
        }
    } else {
        rc = rowVec->erase(begin, end);
        if (rc < 0) {
            return rc;
        }
    }
    // then erase the key and change vector size
    rc = keyVec->erase(begin, end);
    if (rc < 0) {
        return rc;
    }
    return 0;
}

int CBPlusTree::NodeData::erase(const KEY_T& key) noexcept {
    // erase children or rows first
    int begin = keyVec->search(key);


    KEY_T foundKey(key.compareTypes);
    int rc = keyVec->at(begin, foundKey);
    if (!(rc == 0 && key == foundKey)) {
        return -ENOENT;
    }
    int end = begin + 1;


    rc = 0;
    if (!nodeData->hdr->leaf) {
        // + 1 for child pointer
        rc = childVec->erase(begin, end + 1);
        if (rc < 0) {
            return rc;
        }
    } else {
        rc = rowVec->erase(begin, end);
        if (rc < 0) {
            return rc;
        }
    }
    // then erase the key and change vector size
    rc = keyVec->erase(begin, end);
    if (rc < 0) {
        return rc;
    }
    return 0;
}


int CBPlusTree::NodeData::printNode() const noexcept {

    #ifdef __BPDEBUG__
    std::cout << "Node Bid: " << self.bid << ", Keys: \n";
    std::cout << "node header : " << " leaf: " << (int)nodeData->hdr->leaf 
              << " childIsLeaf: " << (int)nodeData->hdr->childIsLeaf 
              << " key count: " << nodeData->hdr->count << endl;
    if (nodeData->hdr->leaf) {
        cout << " prev leaf bid: " << nodeData->hdr->prev
              << " next leaf bid: " << nodeData->hdr->next << std::endl; 
    }
    for (size_t i = 0; i < keyVec->size(); ++i) {
        KEY_T key;
        int rc = keyVec->at(i, key);
        if (rc == 0) {
            std::string keyStr;
            for(int i = 0; i < this->keyLen; ++i) {
                
                keyStr.push_back(hex_chars[((key).data[i] >> 4) & 0x0F]);
                keyStr.push_back(hex_chars[((key).data[i]) & 0x0F]);
                keyStr.push_back(' ');
                if (i % 16 == 15 && i != this->keyLen - 1) {
                    keyStr.push_back('\n');
                }
            }

            std::cout << keyStr << " ";
        }

        // print left child if index node
        if (!nodeData->hdr->leaf) {
            std::cout << " | Child Bid: " << childVec->at(i) << endl;
        } else {
            char rowDataPreview[33] = {0};
            uint8_t* rowData = nullptr;
            uint32_t rowLen = 0;
            rc = rowVec->reference_at(i, rowData, &rowLen);
            if (rc == 0) {
                size_t previewLen = rowLen < 16 ? rowLen : 16;
                for (size_t j = 0; j < previewLen; ++j) {
                    rowDataPreview[j * 2] = hex_chars[(rowData[j] >> 4) & 0x0F];
                    rowDataPreview[j * 2 + 1] = hex_chars[rowData[j] & 0x0F];
                }
                rowDataPreview[previewLen * 2] = '\0';
                std::cout << " | Row Data (len " << rowLen << "): " << rowDataPreview << endl;
            }
        }
    }
    #endif

    return 0;
}


CBPlusTree::iterator::iterator(CBPlusTree& tree, const bidx& start) : 
        m_tree(tree), 
        valid(true),
        node(tree.m_page, tree.keyLen, tree.m_pageSize, tree.m_rowPageSize, tree.m_rowLen),
        m_currentNode(start) {
            m_currentPos = 0;
}

CBPlusTree::iterator::iterator(const iterator& other) : 
        m_tree(other.m_tree), 
        valid(other.valid),
        node(other.m_tree.m_page, other.m_tree.keyLen, other.m_tree.m_pageSize, other.m_tree.m_rowPageSize, other.m_tree.m_rowLen) {
            m_currentNode = other.m_currentNode;
            m_currentPos = other.m_currentPos;
}


CBPlusTree::iterator& CBPlusTree::iterator::operator++() {
    if (m_currentPos + 1 >= node.keyVec->size()) {
        // move to next node
        int rc = 0;
        bidx nextNodeIdx{m_currentNode.gid, node.nodeData->hdr->next};
        if (nextNodeIdx.bid == 0) {
            // end of iteration
            m_currentNode = {0, 0};
            m_currentPos = 0;
            rc = node.deInitNode();
            if (rc != 0) throw std::runtime_error("Failed to de-initialize node for iterator");
            loaded = false;
            return *this;
        }
        m_currentNode = nextNodeIdx;
        m_currentPos = 0;
        rc = node.deInitNode();
        if (rc != 0) throw std::runtime_error("Failed to de-initialize node for iterator");
        loaded = false;
        rc = loadNode();
        if (rc != 0) {
            throw std::runtime_error("Failed to load next node for iterator");
        }
    } else {
        ++m_currentPos;
    }
    return *this;
}

CBPlusTree::iterator& CBPlusTree::iterator::operator--() {
    if (m_currentPos == 0) {
        // move to previous node
        int rc = 0;
        bidx prevNodeIdx{m_currentNode.gid, node.nodeData->hdr->prev};
        if (prevNodeIdx.bid == 0) {
            // end of iteration
            m_currentNode = {0, 0};
            m_currentPos = 0;
            rc = node.deInitNode();
            if (rc != 0) throw std::runtime_error("Failed to de-initialize node for iterator");
            loaded = false;
            return *this;
        }
        m_currentNode = prevNodeIdx;
        m_currentPos = node.keyVec->size() - 1;
        rc = node.deInitNode();
        if (rc != 0) throw std::runtime_error("Failed to de-initialize node for iterator");
        loaded = false;
        rc = loadNode();
        if (rc != 0) {
            throw std::runtime_error("Failed to load previous node for iterator");
        }
    } else {
        --m_currentPos;
    }
    return *this;
}

CBPlusTree::iterator& CBPlusTree::iterator::operator=(const CBPlusTree::iterator& other) {
    if (this != &other) {
        m_currentNode = other.m_currentNode;
        m_currentPos = other.m_currentPos;
        node.deInitNode();
        loaded = false;
        // int rc = loadNode();
        // if (rc != 0) {
        //     throw std::runtime_error("Failed to load start node for iterator");
        // }
    }            
    return *this;
}

int CBPlusTree::iterator::loadData(void* outKey, uint32_t outKeyLen, uint32_t& keyLen, void* outRow, uint32_t outRowLen, uint32_t& rowLen) {
    int rc = loadNode();
    if (rc != 0) {
        return rc;
    }

    KEY_T key(this->m_tree.cmpTyps);
    uint8_t* rowData = nullptr;
    uint32_t actualLen = 0;

    uint32_t copyLen = 0;

    // must be leaf node, otherwise iterator is invalid
    nodeLocker nl(node.pCache, m_tree.m_page, node, true, m_tree.cmpTyps);
    CTemplateReadGuard g(nl);
    if (g.returnCode() != 0) {
        return -EIO;
    }


    if (outKey != nullptr) {
        rc = node.keyVec->at(m_currentPos, key);
        if (rc != 0) {
            return rc;
        }

        keyLen = key.len;
        copyLen = key.len < outKeyLen ? key.len : outKeyLen;
        std::memcpy(outKey, key.data, copyLen);
    }

    if (outRow != nullptr) {
        rc = node.rowVec->reference_at(m_currentPos, rowData, &actualLen);
        if (rc != 0) {
            return rc;
        }
        copyLen = actualLen < outRowLen ? actualLen : outRowLen;
        std::memcpy(outRow, rowData, copyLen);
        rowLen = actualLen;
    }

    return 0;

#ifdef __BPDEBUG__
    // return key and row data
    std::string keyStr;
    for(uint32_t i = 0; i < key.len; ++i) {

        keyStr.push_back(hex_chars[((key).data[i] >> 4) & 0x0F]);
        keyStr.push_back(hex_chars[(key).data[i] & 0x0F]);
        keyStr.push_back(' ');
    }
    std::cout << "Iterator Key: " << keyStr;
    

    char rowDataPreview[128] = {0};

    if (rc == 0) {
        size_t previewLen = actualLen < 32 ? actualLen : 32;
        for (size_t j = 0; j < previewLen; ++j) {
            rowDataPreview[j * 2] = hex_chars[(rowData[j] >> 4) & 0x0F];
            rowDataPreview[j * 2 + 1] = hex_chars[rowData[j] & 0x0F];
        }
        rowDataPreview[previewLen * 2] = '\0';
        std::cout << " | Row Data (len " << actualLen << "): " << rowDataPreview << std::endl;
    }

#endif
    return 0;
}

int CBPlusTree::iterator::loadNode() {
    if (loaded) {
        return 0;
    }
    if (!(m_currentNode == bidx {0, 0})) {
        node.deInitNode();
        int rc = m_tree.load_node(m_currentNode, node, true);
        if (rc != 0) {
            return -EIO;
        }
    } else {
        return -ENOENT;
    }
    loaded = true;
    return 0;
}


nodeLocker::nodeLocker(cacheStruct*& cs, CPage& pge, CBPlusTree::NodeData& node, bool isLeaf, const std::vector<std::pair<uint8_t, dpfs_datatype_t>>& keyType) : 
cacheLocker(cs, pge), 
m_nd(node), 
m_isLeaf(isLeaf), 
m_keyType(keyType) {
    // constructor body can be empty
}

nodeLocker::~nodeLocker() {
    // destructor body can be empty
}

int nodeLocker::reinitStruct() {
    int rc = 0;
    rc = m_nd.deInitNode(); if (rc != 0) return rc;
    rc = m_nd.initNodeByLoad(m_isLeaf, m_nd.maxKeySize, getPtr(), m_keyType); 

    return rc;
}

// class nodeLocker : public cacheLocker {
// public:
//     nodeLocker();
//     virtual ~nodeLocker();
//     virtual int reinitStruct() override;
//     CBPlusTree::NodeData& m_nd;
//     bool m_isLeaf;
//     const std::vector<std::pair<uint8_t, dpfs_datatype_t>>& m_keyType;
// };