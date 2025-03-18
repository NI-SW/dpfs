// compile app with spdk feature
#include "test.hpp"



int main(int argc, char* argv[]) {
    
    // struct spdk_env_opts opts;
    struct spdk_app_opts app_opts = {};
    spdk_app_opts_init(&app_opts, sizeof(struct spdk_app_opts));

    // use if this program run in lib
    // spdk_env_opts_init(&opts); 
    // spdk_env_init(&opts);

    // init app_opts
    app_opts.name = "Mr.Li";
    app_opts.rpc_log_file = fopen("./test.log", "w");
    app_opts.rpc_log_level = SPDK_LOG_DEBUG;
    app_opts.interrupt_mode = true;
    app_opts.json_config_file = argv[1];

    myctx *s = (myctx*)malloc(sizeof(myctx));


    // start spdk reactor and this program will be hold until spdk_app_stop() called
    int rc = spdk_app_start(&app_opts, test_entry, s);
    if(rc != 0) {
        log_info("spdk_app exit with error");
    }



    return 0;
}

