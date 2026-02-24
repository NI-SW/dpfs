/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#include <collect/collect.hpp>
#include <thread>
#include <iostream>
#include <storage/nvmf/nvmf.hpp>
#include <collect/page.hpp>
using namespace std;
std::thread test;


int main() {
	CNvmfhost* engine = new CNvmfhost();
	engine->log.set_log_path("./logbinary.log");

	std::vector<dpfsEngine*> engine_list;
	engine_list.emplace_back(engine);

	CPage* pge = new CPage(engine_list, 100, engine->log);

	CDiskMan dm(pge);

	CCollection c(dm, *pge);


	cout << "Endian: " << B_END << endl;
	delete engine;
	return 0;
}



