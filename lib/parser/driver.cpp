#include <parser/driver.hpp>

dpfsDriver::dpfsDriver(CUser& usr) : parser(usr) {

};

dpfsDriver::~dpfsDriver() {

}

/*
    @param sql: SQL string to execute
    @return 0 on success, else on failure
*/
int dpfsDriver::execute(const std::string& sql) {
    int rc = 0;
    // create temp collection and get first iterator
    rc = parser(sql);
    if (rc != 0) {
        goto errReturn;
    }

    return 0;

errReturn:

    if (m_pTempCollection) {
        delete m_pTempCollection;
        m_pTempCollection = nullptr;
    }

    return rc;
}

/*
    @return 0 on success, else on failure
    @note fetch next row
*/
int dpfsDriver::fetchNext() {
    if(!m_pCursor) {
        return -EIO;
    }

    ++(*m_pCursor);
    return 0;
}

/*
    @return 0 on success, else on failure
    @note initialize the driver
*/
int dpfsDriver::initializeDriver() { 


    return 0;
}
/*
    @return 0 on success, else on failure
    @note close the cursor
*/
int dpfsDriver::closeCursor() {
    if(m_pCursor) {
        delete m_pCursor;
        m_pCursor = nullptr;
    }
    if(m_pTempCollection) {
        delete m_pTempCollection;
        m_pTempCollection = nullptr;
    }
    return 0;
}