/*
 * Auth: Libr9926@gmail.com
 * update: 2024-9-9
 * 
 * describe: header of client struct and APIs
*/

/*
客户端：
功能
1.连接到分布式文件系统
2.断开与分布式文件系统的连接
3.发出请求
使用分布式文件系统，可选请求如下
1.读写文件（支持部分类似fstream的接口，提供高性能文件读写，每次请求一个块？跨块访问再请求一个块？小文件直接传输整个文件？）
2.下载文件
3.查看文件目录
4.查看文件大小

PS: 客户端只连接控制节点
*/
#include <dn.h>

class Client {
    CDfsnet conn;
    
};