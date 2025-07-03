/*
 * Auth: Libr9926@gmail.com
 * update: 2024-9-9
 * 
 * describe: header of dfs network struct and APIs
*/


/*
 * 1.TCP processing data packages
*/

class CDfsnet {
public:
    CDfsnet(){};
    int connect(const char* ip, int port);
    int disconnect();

};