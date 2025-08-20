/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
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