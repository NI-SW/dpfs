
#include <spdk/scsi.h>
#include <spdk/env.h>
#include <spdk/event.h>
#include <spdk/config.h>
#include <spdk/stdinc.h>
#include <spdk/blob.h>
#include <spdk/bdev.h>
#include <spdk/blob_bdev.h>
#include <string>

void test_entry(void *arg);
// init blob store dev
static void my_bs_init_complete(void *ctx, struct spdk_blob_store *bs, int bserrno);
static void my_create_bs(enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *event_ctx);
static void my_create_blob(void *cb_arg, spdk_blob_id blobid, int bserrno);

/*
app_start->bs_init->blob_create->blob_open->blob_write->blob_read->blob_close->blob_destroy->bs_unload->app_stop

*/

template<typename T>
void log_info(const T& s) noexcept {
    SPDK_PRINTF("%s\n", s);
    return;
}

struct myctx{
    spdk_bs_dev *bdev;
    spdk_blob_id blobid;
    spdk_blob_store *bs;
};

// create blob 
static void my_create_blob(void *cb_arg, spdk_blob_id blobid, int bserrno) {
    log_info((char*)cb_arg);
    blobctx *ctx = (blobctx*)cb_arg;
    ctx->blobid = blobid;
    spdk_bs_open_blob(ctx->bs, blobid, my_open_blob, ctx);

    return;
}


static void my_create_bs(enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *event_ctx) {
    log_info("test_bs_callback() called");

    return;

}

// spdk_bs_op_with_handle_complete 
// spdk blob store init complete callback
static void my_bs_init_complete(void *ctx, struct spdk_blob_store *bs, int bserrno) { // init blob store
    log_info((char*)ctx);
    struct spdk_blob_store *bs = (struct spdk_blob_store*)malloc(sizeof(struct spdk_blob_store));
    spdk_bs_create_blob(bs, my_create_blob, (void*)"BS created");
    return;
}

// spdk main function
void test_entry(void *arg) {
    char* s = (char*)arg;
    log_info("start_entry() called");
    log_info(s);

    myctx *ctx = (myctx*)arg;

    // create a blob store device
    struct spdk_bs_dev *bdev = NULL;
    // name from json config file
    const char* bdevName = "Malloc0";
    // create blob store device
    int rc = spdk_bdev_create_bs_dev_ext(bdevName, my_create_bs, NULL, &bdev);
    if(rc != 0) {
        log_info("spdk_bdev_create_bs_dev_ext() failed");
        spdk_app_stop(-1);
    }
    // initialize blob store
    spdk_bs_init(bdev, NULL, my_bs_init_complete, (void*)"test_bs_init");



    sleep(3);
    spdk_app_stop(-1);
    
    // if use scsi device, need to init scsi
    spdk_scsi_task task;
    if(spdk_scsi_init() != 0) {
        log_info("spdk_scsi_init() failed");
    }
    log_info("here is centos 9");


    spdk_scsi_fini();
}

