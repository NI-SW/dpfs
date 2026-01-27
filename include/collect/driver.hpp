#include <collect/collect.hpp>
#include <collect/bp.hpp>
#include <parser/dpfsparser.hpp>

class dpfsDriver {

    dpfsDriver() = default;
    ~dpfsDriver();

    /*
        @param query: query string to execute
        @return 0 on success, else on failure
    */
    int executeQuery(const std::string& query);

    /*
        @note fetch next row
        @return 0 on success, else on failure
    */
    int fetchNext();


    /*
        @return 0 on success, else on failure
        @note initialize the driver
    */
    int initializeDriver();

    /*
        @return 0 on success, else on failure
        @note close the cursor
    */
    int closeCursor();

private:
    yy::CParser parser;
    CCollection* m_pTempCollection = nullptr;
    CBPlusTree::iterator* m_pCursor = nullptr;
    
};