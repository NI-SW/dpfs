#include <dpfssys/user.hpp>

// TODO FINISH PRIVILEGE MANAGEMENT



CUser::CUser(CUser&& other) {
    this->userid = other.userid;
    this->username = std::move(other.username);
    this->currentSchema = std::move(other.currentSchema);
    this->dbprivilege = other.dbprivilege;
}

CUser& CUser::operator=(CUser&& other) {
    this->userid = other.userid;
    this->username = std::move(other.username);
    this->currentSchema = std::move(other.currentSchema);
    this->dbprivilege = other.dbprivilege;
    return *this;
}
