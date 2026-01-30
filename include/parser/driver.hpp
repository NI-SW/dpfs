#include <collect/collect.hpp>
#include <collect/bp.hpp>
#include <parser/dpfsparser.hpp>

class dpfsDriver {
public:
    dpfsDriver(CUser& usr);
    ~dpfsDriver();

    /*
        @param sql: SQL string to execute
        @return 0 on success, else on failure
    */
    int execute(const std::string& sql);

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