/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#include <collect/collect.hpp>
#include <thread>
#include <iostream>
#include <storage/nvmf/nvmf.hpp>
using namespace std;
std::thread test;


int main() {
	CNvmfhost* engine = new CNvmfhost();
	engine->log.set_log_path("./logbinary.log");

	CCollection c(*engine);


	cout << "Endian: " << B_END << endl;
	delete engine;
	return 0;
}



