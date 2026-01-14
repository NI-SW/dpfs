#include <collect/bp.hpp>

CBPlusTree::CBPlusTree(CCollection& collection, CPage& pge, CDiskMan& cdm, size_t pageSize)
    :   m_pageSize(pageSize),
        m_page(pge),
        m_diskman(cdm),
        m_collection(collection),
        high(m_collection.m_collectionStruct->ds->m_btreeHigh),
        m_root(collection.m_collectionStruct->ds->m_dataRoot) {

    // find primary key column, now only support single primary key
    const CColumn& pkCol = m_collection.m_collectionStruct->m_cols[m_collection.m_pkPos];
    if(pkCol.getLen() <= maxSearchKeyLen) {
        keyLen = static_cast<uint8_t>(pkCol.getLen());
    } else {
        keyLen = maxSearchKeyLen;
    }
    keyType = pkCol.getType();
    maxkeyCount = ((m_pageSize * dpfs_lba_size - sizeof(NodeData::nd::hdr_t)) - sizeof(uint64_t)) / (keyLen + sizeof(uint64_t));

    // ditermine order number by key size and page size




    // reserve 1 for split
    m_indexOrder = static_cast<size_t>(maxkeyCount) - 1;
    // m_nodeSize = (m_indexOrder * keyLen) + ((m_indexOrder + 1) * sizeof(uint64_t)) + sizeof(NodeData::nd::hdr_t));

    for(int i = 0; i < m_collection.m_collectionStruct->m_cols.size(); ++i) {
        const CColumn& col = m_collection.m_collectionStruct->m_cols[i];

        if(isVariableType(col.getType())) {
            m_colVariable = true;
        }
        uint32_t dataLen = col.getLen();
        // TODO:
        // limit in-row data length, if data len larger than maxInrowLen, store pointer, offset and length only
        // (other columns data...)(data1|pointer|offset of the newBlock|)(other columns data...)
        // newBlock(|block chain len uint32_t 4B|remain size uint32_t 4B|(valid Indicator(1B)|len4B|data1)|data2|data3|data4|)
        // when delete occur, the newBlock will be compacted to remove invalid data
        // use block chain to store newBlick list
        if(dataLen > maxInrowLen) {
            dataLen = maxInrowLen;
        }
        m_rowLen += dataLen;
    }

    // m_leafRowCount = (m_pageSize * dpfs_lba_size - hdrSize) / (keyLen + m_rowLen);
    size_t rowLenInByte = m_indexOrder * (keyLen + m_rowLen) + sizeof(NodeData::nd::hdr_t);

    
    
    m_rowPageSize = static_cast<uint8_t>( rowLenInByte % dpfs_lba_size == 0 ? rowLenInByte / dpfs_lba_size : (rowLenInByte / dpfs_lba_size) + 1 );
    m_rowOrder = m_indexOrder; // ((m_pageSize * dpfs_lba_size - hdrSize) / (keyLen + m_rowLen)) - 1;

#ifdef __DEBUG__
    m_indexOrder = indOrder;
    m_rowOrder = rowOrder;
#endif

}

CBPlusTree::~CBPlusTree() {
    if(m_commitCache.size() > 0) {
        // rollback uncommitted nodes
        for(auto& it : m_commitCache) {
            #ifdef __DEBUG__
            cout << "Rolling back node gid: " << it.first.gid << " bid: " << it.first.bid << endl;
            #endif
            it.second.pCache->lock();
            it.second.pCache->setStatus(cacheStruct::INVALID);
            it.second.pCache->unlock();
            it.second.pCache->release();
        }
        m_commitCache.clear();
    }

    
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
    KEY_T upKey(tempData, key.len);
    bidx upChild{0, 0};
    // if split is required, rc == SPLIT

    // search in root first
    rc = insert_recursive(m_root, key, row, len, upKey, upChild, high == 1 ? true : false);
    if (rc < 0) return rc;
    if (rc == SPLIT) {


        // root is leaf >> 
        if(high == 1) {
            // create new root node, old root become left child, upChild become right child
            NodeData newRoot(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
            rc = newRoot.initNode(false, m_indexOrder, keyType);
            if(rc != 0) {
                return rc;
            }
            newRoot.self = allocate_node(false);
            if (newRoot.self.bid == 0) return -ENOSPC;

            newRoot.nodeData->hdr->childIsLeaf = 1;

            // load old root as left child
            // CBPlusTree::NodeData oldRoot(m_page);
            // rc = load_node(m_root, oldRoot, true);
            // if(rc != 0) {
            //     return rc;
            // }

            // insert upKey and children
            rc = newRoot.insertChild(upKey, m_root.bid, upChild.bid);
            if(rc != 0) {
                return rc;
            }

            m_root = newRoot.self;
            // newRoot.nodeData->hdr->parent = 0;
            // oldRoot.nodeData->hdr->parent = newRoot.self.bid;
            // upChild.nodeData->hdr->parent = newRoot.self.bid;

            // rc = store_node(oldRoot);
            // if(rc != 0) {
            //     return rc;
            // }
            rc = store_node(newRoot);
            if(rc != 0) return rc;
            

            ++high;
            return 0;
        }

        // root is index node >>

        // if split occur in root node, create new root node, and tree high increase by 1
        // CBPlusTree::NodeData root(m_page);

        // load root
        // rc = load_node(m_root, root, false);
        // if(rc != 0) return rc;

        // // left child is old one, new child is upChild
        // rc = root.insertChild(upKey, 0, upChild.bid);
        // if(rc != 0) return rc;

        // // check if need to split root node
        // if(root.keyVec->size() < m_indexOrder) {
        //     return store_node(root);
        // }

        // split root node
        NodeData newRoot(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
        
        // right.initNode(false, m_indexOrder);
        newRoot.initNode(false, m_indexOrder, keyType);

        // rc = split_internal(root, right, upKey);
        // if(rc != 0) return rc;

        newRoot.self = allocate_node(false);
        if (newRoot.self.bid == 0) return -ENOSPC;

        rc = newRoot.insertChild(upKey, m_root.bid, upChild.bid);
        if(rc != 0) return rc;

        m_root = newRoot.self;

        rc = store_node(newRoot);
        if(rc != 0) return rc;
        
        ++high;
        return 0;
    }
    return 0;
}

int CBPlusTree::search(const KEY_T& key, void* out, uint32_t len, uint32_t* actualLen) {
    CSpinGuard g(m_lock);
    if (m_root.bid == 0) return -ENOENT;
    bidx cur = m_root;
    NodeData node(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
    bool nextIsLeaf = false;
    while (true) {
        int rc = load_node(cur, node, nextIsLeaf);
        if (rc != 0) return rc;
        if (node.nodeData->hdr->leaf) {
            int pos = node.keyVec->search(key);

            KEY_T foundKey;
            rc = node.keyVec->at(pos, foundKey);
            if(!(rc == 0 && foundKey == key)) {
                return -ENOENT;
            }

            return node.rowVec->at(pos, reinterpret_cast<uint8_t*>(out), len, actualLen);
            
        }

        if(node.nodeData->hdr->childIsLeaf) {
            nextIsLeaf = true;
        }

        size_t idx = child_index(node, key);
        cur.bid = node.childVec->at(idx);
    }

    return 0;
}

int CBPlusTree::remove(const KEY_T& key) {
    CSpinGuard g(m_lock);
    if (m_root.bid == 0) return -ENOENT;
    bidx cur = m_root;
    NodeData node(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);

    
    int rc = 0;   
    char tempData[MAXKEYLEN]; 
    KEY_T upKey{tempData, key.len};
    bidx upChild {0, 0};
    // if split is required, rc == SPLIT

    // search in root first
    rc = remove_recursive(m_root, key, upKey, upChild, high == 1 ? true : false);

    if(rc < 0) return rc;
    if(rc == COMBINE) {
        // root need to be combined
        // if root is leaf, do nothing
        if(high == 1) {
            return 0;
        }
    } else if(rc == EMPTY) {
        // tree become empty
        m_root = {0, 0};
        high = 0;
    }
    
    
    
    return 0;
}

int32_t CBPlusTree::remove_recursive(const bidx& idx, const KEY_T& key, KEY_T& upKey, bidx& upChild, bool isLeaf) {
    CBPlusTree::NodeData node(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
    int32_t rc = load_node(idx, node, isLeaf);
    if (rc != 0) return rc;
    int updateType = 0;

    // leaf node founded, inster the data
    if (isLeaf) {

        CTemplateGuard g(*node.pCache);
        if(g.returnCode() != 0) {
            return g.returnCode();
        }

        KEY_T foundKey;
        rc = node.keyVec->at(0, foundKey);
        if(rc == 0 && foundKey == key) {
            // key not found, no need to delete
            node.keyVec->pop_front();
            node.rowVec->pop_front();
            if(node.keyVec->size() > 0) {
                upKey = (*node.keyVec)[0];
                updateType |= UPDATEKEY;
            }
        } else {
            rc = node.erase(key);
            if(rc != 0) return rc;
        }

        
        // check if need to combine
        if(node.size() < (m_rowOrder / 2) && node.self.bid != m_root.bid) {
            // need to combine or borrow from sibling
            rc = store_node(node);
            if(rc != 0) return rc;
            updateType |= COMBINE;
            return updateType;
        }
        rc = store_node(node);
        if (rc != 0) return rc;

        return updateType;
    }

    char tempData[MAXKEYLEN];
    KEY_T childUp{tempData, key.len};
    bidx childNew{nodeId, 0};

    rc = node.pCache->read_lock();
    if(rc != 0) return rc;

    int32_t idxChild = child_index(node, key);
    // TODO CREATE NEW NODE?
    if(idxChild == -ERANGE) {
        // key not found, no need to delete
        node.pCache->read_unlock();
        return 0;
    }
    bidx nextNode = {nodeId, node.childVec->at(idxChild)};
    bool nextIsLeaf = node.nodeData->hdr->childIsLeaf ? true : false;
    node.pCache->read_unlock();

    rc = remove_recursive(nextNode, key, childUp, childNew, nextIsLeaf);
    if (rc < 0) {
        return rc;
    }

    if (rc & UPDATEKEY) {
        // need to update the index key
        CTemplateGuard g(*node.pCache);
        if(g.returnCode() != 0) {
            return g.returnCode();
        }
        int pos = 0;
        pos = node.keyVec->search(key);
        if(pos >= node.keyVec->size()) {
          
        } else if((*node.keyVec)[pos] == key) {
            // update key, and pass the key to parent
            (*node.keyVec)[pos] = childUp;
        }

        updateType |= UPDATEKEY;
        upKey = childUp;

    } 

    if (rc & COMBINE) {
        // need combine two sibling child nodes, or borrow from sibling

        CTemplateGuard g(*node.pCache);
        if(g.returnCode() != 0) {
            return g.returnCode();
        }

        // check wether need to combine child node or borrow from sibling
        rc = combine_child(node, idxChild, childUp, childNew);
        if (rc != 0) return rc;

        // check if need to combine current node
        if(node.size() < m_indexOrder / 2 && node.self.bid != m_root.bid) {
            updateType |= COMBINE;
        }
    } 
    
    rc = store_node(node);
    if(rc != 0) return rc;
    

    return updateType;
}

int32_t CBPlusTree::combine_child(NodeData& node, int32_t idxChild, KEY_T& childUp, bidx& childNew) {

    int leftIdx = idxChild == 0 ? -1 : idxChild - 1;
    int rightIdx = idxChild == node.keyVec->size() ?  -1 : idxChild + 1;

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

    int rc = 0;
    // load left child
    bidx idx = {nodeId, 0}; 

    // load target child node
    rc = load_node({nodeId, node.childVec->at(idxChild)}, target, isLeaf);
    if (rc != 0) return rc;

    // select sibling to borrow or combine
    if(leftIdx != -1) {
        idx.bid = node.childVec->at(leftIdx);
        rc = load_node(idx, left, isLeaf);
        if (rc != 0) return rc;
    }
    
    if(rightIdx != -1) {
        // load right child
        idx.bid = node.childVec->at(rightIdx);
        rc = load_node(idx, right, isLeaf);
        if (rc != 0) return rc;
    }

    if(!left.inited && !right.inited) {
        return -EFAULT;
    } else if(left.inited && !right.inited) {
        borrowNode = &left;
        fromLeft = true;
    } else if(!left.inited && right.inited){
        borrowNode = &right;
        fromLeft = false;
    } else {
        rc = left.pCache->read_lock();
        if(rc != 0) return rc;
        rc = right.pCache->read_lock();
        if(rc != 0) {
            left.pCache->read_unlock();
            return rc;
        }
        
        // both sibling exists, choose larger one
        if(left.size() >= right.size()) {
            borrowNode = &left;
            fromLeft = true;
        } else {
            borrowNode = &right;
            fromLeft = false;
        }
        left.pCache->read_unlock();
        right.pCache->read_unlock();
    }

    // lock two nodes
    CTemplateGuard gt(*target.pCache);
    if(gt.returnCode() != 0) {
        return gt.returnCode();
    }
    // lock two nodes
    CTemplateGuard gb(*borrowNode->pCache);
    if(gb.returnCode() != 0) {
        return gb.returnCode();
    }

    // if sibling has enough keys, borrow from sibling, else merge two nodes
    if(borrowNode->size() > (isLeaf ? (m_rowOrder / 2) : (m_indexOrder / 2))) {
        ct = BORROW;
        #ifdef __DEBUG__
        // std::cout << "BORROW from " << (fromLeft ? "LEFT" : "RIGHT") << " sibling node." << std::endl;
        // std::cout << "  Target Node before borrow: ";
        // target.printNode();
        // std::cout << "  Borrow Node before borrow: ";
        // borrowNode->printNode();
        #endif
    } else {
        ct = MERGE;
    }

    if(ct == BORROW) {
        // borrow from sibling node
        rc = borrowKey(node, *borrowNode, target, isLeaf, fromLeft, idxChild);
        if(rc != 0) return rc;
    } else if(ct == MERGE) {
        // merge two nodes
        rc = mergeNode(node, *borrowNode, target, isLeaf, fromLeft, idxChild);
        if(rc != 0) return rc;

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
    
    
    if(isLeaf) {
        // borrow key from sibling leaf node

        // move key and row data from fromNode to toNode
        KEY_T fromKey;
        int rc = fromNode.keyVec->at(fromKeyIdx, fromKey);
        if(rc != 0) return rc;

        KEY_T parentKey;
        rc = parent.keyVec->at(parentKeyIdx, parentKey);
        if(rc != 0) return rc;
        
        uint8_t* rowData = nullptr;
        uint32_t rowLen = 0;
        // copy borrow data to target
        rc = fromNode.rowVec->reference_at(fromKeyIdx, rowData, &rowLen);
        if(rc != 0) return rc;


        // change parent key
        if(fromLeft) {
            rc = toNode.pushFrontRow(fromKey, rowData, rowLen);
            if(rc != 0) return rc;
            // left borrow use last key
            parentKey = fromKey;
            fromNode.popBackRow();
        } else {
            // may cause 2 parent key change
            
            rc = toNode.pushBackRow(fromKey, rowData, rowLen);
            if(rc != 0) return rc;
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
        KEY_T fromKey;
        // get key from sibling node
        int rc = fromNode.keyVec->at(fromKeyIdx, fromKey);
        if(rc != 0) return rc;

        KEY_T parentKey;
        // get change key from parent node
        rc = parent.keyVec->at(parentKeyIdx, parentKey);
        if(rc != 0) return rc;
        
        char tempData[MAXKEYLEN];
        KEY_T targetKey{tempData, parentKey.len};
        targetKey = parentKey;
        
        // get target insert key from target node

        // rc = toNode.keyVec->at(fromLeft ? 0 : toNode.keyVec->size() - 1, targetKey);
        // if(rc != 0) return rc;
        // *targetKey = *parentKey;

        parentKey = fromKey;

        child_t fromNodeChild = 0;
        // if from left, get last child, else get first child
        fromNodeChild = fromNode.childVec->at(fromLeft ? fromNode.size() : 0);
        if(rc != 0) return rc;

        // insert child into target node
        if(fromLeft) {
            // insert to left
            rc = toNode.pushFrontChild({targetKey.data, targetKey.len}, fromNodeChild);
            if(rc != 0) return rc;
            fromNode.popBackChild();
        } else {

            // insert to right
            rc = toNode.pushBackChild({targetKey.data, targetKey.len}, fromNodeChild);
            if(rc != 0) return rc;
            fromNode.popFrontChild();
        }
    }
    return 0;
}

int CBPlusTree::mergeNode(NodeData& parent, NodeData& fromNode, NodeData& toNode, bool isLeaf, bool fromLeft, int targetIdx) {

    KEY_T parentKey;
    int rc = 0;
    
    NodeData* rightNode = &toNode;
    NodeData* leftNode = &fromNode;
    int parentKeyIdx = targetIdx - 1;

    if(!fromLeft) {
        rightNode = &fromNode;
        leftNode = &toNode;
        parentKeyIdx = targetIdx;
    }

    rc = parent.keyVec->at(parentKeyIdx, parentKey);
    if(rc != 0) return rc;


    if(isLeaf) {

        // delete the right node, move right node data to left node
        rc = leftNode->pushBackRows(*rightNode);
        if(rc != 0) return rc;
        // will also delete right child of the parent key
        parent.erase(parentKeyIdx, parentKeyIdx + 1);
        if(rc != 0) return rc;

        // delete right child node
        rightNode->needDelete = true;
        store_node(*rightNode);
        
        // rc = free_node(rightNode->self, isLeaf);
        // if(rc != 0) return rc;
        // rightNode->pCache->release();

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
    if(rc != 0) return rc;

    //  [  1  5  8  13  16  20  27 ] => [  1  5  13  16  20  27]
    rc = parent.erase(parentKeyIdx, parentKeyIdx + 1);
    if(rc != 0) return rc;

    // delete right child node
    rightNode->needDelete = true;
    store_node(*rightNode);

    // rc = free_node(rightNode->self, isLeaf);
    // if(rc != 0) return rc;
    // rightNode->pCache->release();

    if(parent.size() == 0 && parent.self.bid == m_root.bid) {
        // root node become empty, update m_root
        m_root = leftNode->self;
        --high;
    }



    return 0;
}

#ifdef __DEBUG__


void CBPlusTree::printTree() {
    CSpinGuard g(m_lock);
    if (m_root.bid == 0) {
        std::cout << "B+ Tree is empty." << std::endl;
        return;
    }
    printTreeRecursive(m_root, high == 1 ? true : false, high);

}

const char hex_chars[] = "0123456789ABCDEF";

void CBPlusTree::printTreeRecursive(const bidx& idx, bool isLeaf, int level) {
    NodeData node(m_page, keyLen, m_pageSize, m_rowPageSize, m_rowLen);
    int rc = load_node(idx, node, isLeaf);
    if (rc != 0) {
        std::cout << "Failed to load node at bid " << idx.bid << std::endl;
        return;
    }

    std::cout << "----------------------------------------" << std::endl;
    std::cout << "Level " << (high - level + 1) << " - Node Bid: " << idx.bid << ", Keys: \n";
    std::cout << "node header : " << " leaf: " << (int)node.nodeData->hdr->leaf 
              << " childIsLeaf: " << (int)node.nodeData->hdr->childIsLeaf 
              << " key count: " << node.keyVec->size() << endl;
    if(node.nodeData->hdr->leaf) {
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
            }

            std::cout << keyStr << " ";
        }

        // print left child if index node
        if(!node.nodeData->hdr->leaf) {
            std::cout << " | Child Bid: " << node.childVec->at(i) << endl;
        } else {
            char rowDataPreview[48] = {0};
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

#endif




// int CBPlusTree::NodeData::initNode(bool isLeaf, int32_t order) {
//     if(isLeaf) {
//         nodeData = new nd(isLeaf);
//     } else {
//         nodeData = new nd(isLeaf);
//     }

//     keys = (reinterpret_cast<KEY_T*>(nodeData->data + sizeof(NodeData::nd::hdr_t)));
//     keyVec = new CKeyVec(*this);

//     if (keyVec == nullptr) {
//         return -ENOMEM;
//     }

//     if (!isLeaf) {
//         // pass header and keys area
//         // constexpr uint32_t order32 = (PAGESIZE * 4096 - sizeof(NodeData::nd::hdr_t) - sizeof(uint64_t)) / (KEYLEN + sizeof(uint64_t));
//         children = (reinterpret_cast<uint64_t*>(nodeData->data + sizeof(NodeData::nd::hdr_t) + (KEYLEN * order)));
//         childVec = new CChildVec(*this);
//         if (childVec == nullptr) {
//             return -ENOMEM;
//         }
//     }
//     else {
//         // values = (reinterpret_cast<void*>(nodeData->data + sizeof(nodeData->hdr) + (KEYLEN * order)));
//         // values = reinterpret_cast<void*>(keys);
//         rowVec = new CRowVec(*this, order);
//         if (rowVec == nullptr) {
//             return -ENOMEM;
//         }
//     }
//     inited = true;
//     return 0;
// }

int CBPlusTree::NodeData::initNode(bool isLeaf, int32_t order, dpfs_datatype_t keyType) {

    void* zptr = isLeaf ? m_page.alloczptr(m_rowPageSize) : m_page.alloczptr(m_pageSize);
    if(zptr == nullptr) {
        return -ENOSPC;
    }

    nodeData = new nd(isLeaf, zptr, m_pageSize, m_rowPageSize);
    memset(nodeData->data, 0, isLeaf ? m_rowPageSize : m_pageSize);
    nodeData->hdr->leaf = isLeaf ? 1 : 0;
    
    maxKeySize = order;
    keys = (reinterpret_cast<uint8_t*>(nodeData->data + sizeof(NodeData::nd::hdr_t)));
    keyVec = new CKeyVec<KEY_T, uint16_t>(keys, nodeData->hdr->count, keyLen, maxKeySize + 1, keyType);

    if (keyVec == nullptr) {
        return -ENOMEM;
    }

    if (!isLeaf) {
        // pass header and keys area
        // constexpr uint32_t order32 = (PAGESIZE * 4096 - sizeof(NodeData::nd::hdr_t) - sizeof(uint64_t)) / (KEYLEN + sizeof(uint64_t));
        children = (reinterpret_cast<uint64_t*>(nodeData->data + sizeof(NodeData::nd::hdr_t) + (keyLen * order)));
        childVec = new CChildVec(*this);
        if (childVec == nullptr) {
            return -ENOMEM;
        }
    }
    else {

        // values = (reinterpret_cast<void*>(nodeData->data + sizeof(nodeData->hdr) + (KEYLEN * order)));
        // values = reinterpret_cast<void*>(keys);
        rowVec = new CRowVec(*this, m_rowLen);
        if (rowVec == nullptr) {
            return -ENOMEM;
        }
    }

    inited = true;
    return 0;
}

int CBPlusTree::NodeData::initNodeByLoad(bool isLeaf, int32_t order, void* zptr, dpfs_datatype_t keyType) {

    nodeData = new nd(isLeaf, zptr, m_pageSize, m_rowPageSize);
    if(nodeData == nullptr) {
        return -ENOMEM;
    }

    maxKeySize = order;
    keys = (reinterpret_cast<uint8_t*>(nodeData->data + sizeof(NodeData::nd::hdr_t)));
    keyVec = new CKeyVec<KEY_T, uint16_t>(keys, nodeData->hdr->count, keyLen, maxKeySize + 1, keyType);

    if (keyVec == nullptr) {

        return -ENOMEM;
    }

    if (!isLeaf) {
        // pass header and keys area
        children = (reinterpret_cast<uint64_t*>(nodeData->data + sizeof(NodeData::nd::hdr_t) + (keyLen * order)));
        childVec = new CChildVec(*this);
        if (childVec == nullptr) {
            return -ENOMEM;
        }
    } else {
        // values = (reinterpret_cast<void*>(nodeData->data + sizeof(nodeData->hdr) + (KEYLEN * order)));
        // values = reinterpret_cast<void*>(keys);
        rowVec = new CRowVec(*this, order);
        if (rowVec == nullptr) {
            return -ENOMEM;
        }
    }

    inited = true;
    return 0;
}

int CBPlusTree::NodeData::insertChild(const KEY_T& key, uint64_t lchild, uint64_t rchild) {
    if (nodeData->hdr->leaf) {
        // is leaf node insert child is invalid
        return -EINVAL;
    }
    int pos = 0;
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
    if(lchild) {
        if (pos == childVec->size()) {
            // insert at the end
            childVec->push_back(lchild);
        } else if(pos < childVec->size()) {
            childVec->insert(pos, lchild);
        } else {
            return -EFAULT;
        }
        
        // childVec->at(pos) = lchild;

    }

    if (pos + 1 == childVec->size()) {
        // insert at the end
        childVec->push_back(rchild);
    } else if(pos + 1 < childVec->size()) {
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

    if(isRef) {
        return;
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
        if(!pCache && nodeData->data) {
            m_page.freezptr(nodeData->data, nodeData->hdr->leaf ? m_rowPageSize : m_pageSize);
            nodeData->data = nullptr;
        }
        delete nodeData;
        nodeData = nullptr;
    }

    if(pCache) {
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


    KEY_T foundKey;
    int rc = keyVec->at(begin, foundKey);
    if(!(rc == 0 && foundKey == key)) {
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

    #ifdef __DEBUG__
    std::cout << "Node Bid: " << self.bid << ", Keys: \n";
    std::cout << "node header : " << " leaf: " << (int)nodeData->hdr->leaf 
              << " childIsLeaf: " << (int)nodeData->hdr->childIsLeaf 
              << " key count: " << nodeData->hdr->count << endl;
    if(nodeData->hdr->leaf) {
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
            }

            std::cout << keyStr << " ";
        }

        // print left child if index node
        if(!nodeData->hdr->leaf) {
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