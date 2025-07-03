#include <storage/spdkcontrol.hpp>
#include <spdk/scsi.h>
#include <spdk/env.h>
#include <spdk/event.h>
#include <spdk/config.h>
#include <spdk/stdinc.h>
#include <spdk/blob.h>
#include <spdk/bdev.h>
#include <spdk/blob_bdev.h>

/*
app_start->bs_init->blob_create->blob_open->blob_write->blob_read->blob_close->blob_destroy->bs_unload->app_stop

reactor -> deal all the callback function

组织结构体，控制spdk的行为，以SPDK为核心，构建存储引擎


块设备：



*/

struct spdkctx {
    // blob store device pointer
    spdk_bs_dev *bsdev;
    // blob id
    spdk_blob_id blobid;
    // blob store pointer
    spdk_blob_store *bs;
    // driver pointer
    spdk_pci_driver *driver;
    // spdk bdev
    spdk_bdev *bdev;
    spdk_bdev_desc* bdev_desc;
    spdk_io_channel* bdev_io_channel;
    // spdk thread handle
    spdk_thread *thd;
};

static void my_bs_init_cb       (void *ctx, struct spdk_blob_store *bs, int bserrno);
static void my_bdev_event_cb   (enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *event_ctx);
static void my_create_blob      (void *cb_arg, spdk_blob_id blobid, int bserrno);
static void my_bdev_open_cb     (struct spdk_bdev_desc *desc, int rc, void *cb_arg);
// static int  my_dev_init         (void *enum_ctx, struct spdk_pci_device *dev);


// open blob
static void my_open_blob(void *cb_arg, struct spdk_blob *blb, int bserrno) {

    SPDK_NOTICELOG("successfully open blob\n");
    // here write blob
    // spdkctx *ctx = (spdkctx*)cb_arg;


    // spdk_blob_io_write(blb, NULL, 0, 0, 0, my_write_blob, cb_arg);
    

    SPDK_NOTICELOG("start write blob\n");
    return;

}

// create blob 
static void my_create_blob(void *cb_arg, spdk_blob_id blobid, int bserrno) {
    // here create blob
    if(bserrno != 0) {
        SPDK_NOTICELOG("create blob failed\n");
        spdk_app_stop(-1);
    }
    SPDK_NOTICELOG("create blob success\n");

    spdkctx *ctx = (spdkctx*)cb_arg;

    if(blobid == SPDK_BLOBID_INVALID) {
        SPDK_NOTICELOG("create blob failed\n");
        spdk_app_stop(-1);
    }

    ctx->blobid = blobid;
    SPDK_NOTICELOG("blobid:: %lu\n", ctx->blobid);
    spdk_bs_open_blob(ctx->bs, blobid, my_open_blob, ctx);

    return;
}


static void my_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *event_ctx) {
    SPDK_NOTICELOG("my_bdev_event_callback() called\n");
    spdkctx *ctx = (spdkctx*)event_ctx;
    ctx->bdev = bdev;
    // spdk_bs_init(ctx->bsdev, NULL, my_bs_init_cb, (void*)ctx);

    return;
}

// spdk_bs_op_with_handle_complete 
// spdk blob store init complete callback
static void my_bs_init_cb(void *ctx, struct spdk_blob_store *bs, int bserrno) { // init blob store
    // here init blob store
    spdkctx *mctx = (spdkctx*)ctx;
    SPDK_NOTICELOG("blob store init complete\n");
    mctx->blobid = 1;
    mctx->bs = bs;
    spdk_bs_create_blob(bs, my_create_blob, ctx);

    uint64_t bsSize = spdk_bs_get_io_unit_size(mctx->bs);
    SPDK_NOTICELOG("blob store size: %lu\n", bsSize);


    return;
}

static void my_bdev_open_cb(struct spdk_bdev_desc *desc, int rc, void *cb_arg) {
    spdkctx* ctx = (spdkctx*)cb_arg;
    ctx->bsdev->blockcnt = spdk_bdev_get_num_blocks(ctx->bdev);
    SPDK_NOTICELOG("my_bdev_open_cb() called\n");
    if(rc != 0) {
        SPDK_NOTICELOG("open bdev failed\n");
        spdk_app_stop(-1);
    }
    SPDK_NOTICELOG("open bdev success\n");
    return;
    

}

// static void my_dev_init(void *arg) {
//     SPDK_NOTICELOG("my_dev_init() called\n");
//     spdkctx *ctx = (spdkctx*)arg;
//     spdk_bs_dev *bsdev = ctx->bsdev;
//     spdk_bs_load(bsdev, NULL, my_bs_init_cb, ctx);
//     return;
// }

// int my_dev_init(void *enum_ctx, struct spdk_pci_device *dev) {
//     SPDK_NOTICELOG("my_dev_init() called\n");
//     spdkctx *ctx = (spdkctx*)enum_ctx;
//     spdk_pci_addr addr = dev->addr;
//     SPDK_NOTICELOG("device addr: %x:%x:%x.%x\n", addr.domain, addr.bus, addr.dev, addr.func);
//     SPDK_NOTICELOG("test:%lu\n", ctx->blobid);
//     return 0;
// }


void my_open_bdev(void* ctx) {
    // open blob device
    spdkctx* ctx1 = (spdkctx*)ctx;
    int rc = 0;
    spdk_bdev_desc *bdev_desc = NULL;
    
    
    // 异步打开bdev
    rc = spdk_bdev_open_ext("aio0", true, my_bdev_event_cb, ctx1, &ctx1->bdev_desc);
    if(rc != 0) {
        SPDK_NOTICELOG("spdk_bdev_open_async() failed\n");
        return;
    }
    SPDK_NOTICELOG("open blob device with async method success\n");

    ctx1->bdev = spdk_bdev_desc_get_bdev(ctx1->bdev_desc);

	ctx1->bdev_io_channel = spdk_bdev_get_io_channel(ctx1->bdev_desc);
	if (ctx1->bdev_io_channel == NULL) {
		SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
		spdk_bdev_close(ctx1->bdev_desc);
		spdk_app_stop(-1);
		return;
	}


    return;
}

// spdk main function
static void spdk_entry(void *arg) {
    
    SPDK_NOTICELOG("start_entry() called\n");
    spdkctx *ctx = (spdkctx*)arg;
    ctx->thd = spdk_get_thread();
    if(ctx->thd == NULL) {
        SPDK_NOTICELOG("spdk_get_thread() failed\n");
        spdk_app_stop(-1);
    }

// step 1: init blob store device

    
    // create a blob store device
    // struct spdk_bs_dev *bsdev = NULL;
    // name from json config file
    // const char* bsdevName = "Malloc0";
    // const char* bsdevName = "aio0";

    // create blob store device
    // 异步执行，这个函数被调用后，会立即返回，然后等待回调函数被调用，回调函数会放入reactor的调用队列，直到reactor处理到这个回调函数，其他函数也如此
    // 创建bsdev
    // rc = spdk_bdev_create_bs_dev_ext(bsdevName, my_create_bdev_cb, ctx, &bsdev);
    // if(rc != 0) {
    //     SPDK_NOTICELOG("spdk_bdev_create_bs_dev_ext() failed\n");
    //     spdk_app_stop(-1);
    // }
    // SPDK_NOTICELOG("create blob store device success\n");
    

    // SPDK_NOTICELOG("here is centos %d\n", 9);

}


CSpdkControl::CSpdkControl(std::string json_config_file) {
    ctx = new spdkctx;
    app_opts = new spdk_app_opts;
    running = false;
    this->json_config_file = json_config_file.c_str();
}

CSpdkControl::~CSpdkControl() {
    delete ctx;
    delete app_opts;
    ctx = nullptr;
    running = false;
    if(my_thread.joinable()) {
        my_thread.join();
    }

}

void CSpdkControl::start_spdk() {

    // struct spdk_env_opts opts;
    
    *app_opts = {};
    spdk_app_opts_init(app_opts, sizeof(struct spdk_app_opts));

    // use if this program run in lib
    // spdk_env_opts_init(&opts); 
    // spdk_env_init(&opts);

    // init app_opts
    app_opts->name = "Mr.Li";
    app_opts->rpc_log_file = fopen("./dpfs.log", "w");
    app_opts->rpc_log_level = SPDK_LOG_INFO;
    app_opts->interrupt_mode = false; // true;
    // app_opts->interrupt_mode = true;
    app_opts->json_config_file = json_config_file.c_str();
    // app_opts->reactor_mask = "0x1";// only use core 0 

    my_thread = std::thread(spdk_app_start, app_opts, spdk_entry, ctx);

    running = true;
    return;
}

void CSpdkControl::stop_spdk() {
    spdk_app_stop(0);
    my_thread.join();
    running = false;

    return;
}

bool CSpdkControl::active() {
    return running;
}

int CSpdkControl::send_spdk_msg(void(*fn)(void *)) {
    // send msg to spdk thread
    int rc = 0;
    // spdk_thread_send_msg(ctx->thd, (spdk_msg_fn)fn, ctx);
    rc = spdk_thread_send_msg(ctx->thd, fn, ctx);
    if(rc != 0) {
        SPDK_NOTICELOG("send msg failed\n");
        return -1;
    }
    return 0;
}

int CSpdkControl::open_blob_device() {
    // open blob device
    int rc = 0;
    
    // 使用send_msg调用写入或读取
    rc = send_spdk_msg(my_open_bdev);
    if(rc != 0) {
        // SPDK_NOTICELOG("spdk_bdev_open_async() failed\n");
        return -1;
    }
    


    return 0;
}




#include <iostream>

void CSpdkControl::test() {
    spdk_thread* t = spdk_get_thread();
    if(t) {
        std::cout << "get" << std::endl;
    } else {
        std::cout << "not get" << std::endl;
    }
    return;
}