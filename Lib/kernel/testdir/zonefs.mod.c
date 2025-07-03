#include <linux/module.h>
#define INCLUDE_VERMAGIC
#include <linux/build-salt.h>
#include <linux/elfnote-lto.h>
#include <linux/export-internal.h>
#include <linux/vermagic.h>
#include <linux/compiler.h>

BUILD_SALT;
BUILD_LTO_INFO;

MODULE_INFO(vermagic, VERMAGIC_STRING);
MODULE_INFO(name, KBUILD_MODNAME);

__visible struct module __this_module
__section(".gnu.linkonce.this_module") = {
	.name = KBUILD_MODNAME,
	.init = init_module,
#ifdef CONFIG_MODULE_UNLOAD
	.exit = cleanup_module,
#endif
	.arch = MODULE_ARCH_INIT,
};

#ifdef CONFIG_MITIGATION_RETPOLINE
MODULE_INFO(retpoline, "Y");
#endif



static const struct modversion_info ____versions[]
__used __section("__versions") = {
	{ 0x68640092, "inode_init_owner" },
	{ 0x53cc7b30, "iomap_readahead" },
	{ 0xa818fa26, "vfs_fsync_range" },
	{ 0xa0427bfa, "setattr_prepare" },
	{ 0x9d2c5dd3, "filemap_migrate_folio" },
	{ 0xbcfbba5b, "iget_locked" },
	{ 0x79c2b4c3, "__bio_add_page" },
	{ 0x12c0faf4, "iocb_bio_iopoll" },
	{ 0xe1f1d2ed, "new_inode" },
	{ 0x656e4a6e, "snprintf" },
	{ 0xa6257a2f, "complete" },
	{ 0xa5401489, "filemap_map_pages" },
	{ 0x3eea5e77, "iomap_file_buffered_write" },
	{ 0xa3d09579, "trace_raw_output_prep" },
	{ 0x392616f2, "unregister_filesystem" },
	{ 0x48d88a2c, "__SCT__preempt_schedule" },
	{ 0x608741b5, "__init_swait_queue_head" },
	{ 0x8800f2c7, "d_make_root" },
	{ 0xfae0c20c, "trace_event_printf" },
	{ 0x53569707, "this_cpu_off" },
	{ 0x4c236f6f, "__x86_indirect_thunk_r15" },
	{ 0x376148b9, "sb_set_blocksize" },
	{ 0x490456c9, "sync_filesystem" },
	{ 0xf003abea, "d_splice_alias" },
	{ 0x7e117dfd, "trace_event_raw_init" },
	{ 0xb76af1f0, "inode_dio_wait" },
	{ 0x37a0cba, "kfree" },
	{ 0xfc5a7949, "iput" },
	{ 0x505bf06e, "pcpu_hot" },
	{ 0xa7b70f19, "iter_file_splice_write" },
	{ 0x59c31521, "bpf_trace_run2" },
	{ 0xd4881925, "event_triggers_call" },
	{ 0x955d6a9c, "dquot_transfer" },
	{ 0x61338c3b, "register_filesystem" },
	{ 0x3327706d, "kmem_cache_create" },
	{ 0x9ae85011, "iomap_dirty_folio" },
	{ 0xba8fbd64, "_raw_spin_lock" },
	{ 0xc3ff38c2, "down_read_trylock" },
	{ 0xbdfb6dbb, "__fentry__" },
	{ 0xe783e261, "sysfs_emit" },
	{ 0xeecdc82f, "trace_event_buffer_commit" },
	{ 0xf639cae, "iomap_invalidate_folio" },
	{ 0x65487097, "__x86_indirect_thunk_rax" },
	{ 0xcd750ada, "iomap_dio_rw" },
	{ 0xbe086d5e, "kill_block_super" },
	{ 0x87726066, "unlock_new_inode" },
	{ 0x92997ed8, "_printk" },
	{ 0xdf41af54, "blkdev_zone_mgmt" },
	{ 0xf0fdf6cb, "__stack_chk_fail" },
	{ 0x53a89771, "trace_event_ignore_this_pid" },
	{ 0x206d1316, "make_kuid" },
	{ 0x9ab1a548, "fs_kobj" },
	{ 0x112cafbc, "__free_pages" },
	{ 0x5fc2b146, "iomap_page_mkwrite" },
	{ 0x7cd8d75e, "page_offset_base" },
	{ 0x599fb41c, "kvmalloc_node" },
	{ 0x12c3e452, "inode_init_once" },
	{ 0x8861364f, "file_write_and_wait_range" },
	{ 0xad5f0017, "perf_trace_buf_alloc" },
	{ 0x6432a031, "perf_trace_run_bpf_submit" },
	{ 0x57bc19d2, "down_write" },
	{ 0xce807a25, "up_write" },
	{ 0xc7487be1, "generic_file_read_iter" },
	{ 0x8df92f66, "memchr_inv" },
	{ 0x88441b32, "setattr_copy" },
	{ 0x69dd3b5b, "crc32_le" },
	{ 0xc6bcdb21, "file_update_time" },
	{ 0x4dfa8d4b, "mutex_lock" },
	{ 0x6e4000fa, "set_nlink" },
	{ 0xcbab7900, "kmem_cache_free" },
	{ 0x5f3b7d21, "iomap_swapfile_activate" },
	{ 0xd552b2b5, "trace_event_reg" },
	{ 0xd648844a, "kobject_init_and_add" },
	{ 0x5a5a2271, "__cpu_online_mask" },
	{ 0xcefb0c9f, "__mutex_init" },
	{ 0x629d1afa, "bio_init" },
	{ 0x25974000, "wait_for_completion" },
	{ 0x5b8239ca, "__x86_return_thunk" },
	{ 0xc5414ae8, "make_kgid" },
	{ 0xe40c37ea, "down_write_trylock" },
	{ 0x170062e9, "kobject_create_and_add" },
	{ 0x668b19a1, "down_read" },
	{ 0xa82963b4, "truncate_setsize" },
	{ 0x4dcbdc6b, "bpf_trace_run3" },
	{ 0x9bd1a7d7, "iomap_read_folio" },
	{ 0x97651e6c, "vmemmap_base" },
	{ 0x31aafcd5, "generic_read_dir" },
	{ 0x48031aa5, "kobject_del" },
	{ 0x132319ee, "trace_event_buffer_reserve" },
	{ 0x85df9b6c, "strsep" },
	{ 0x7e7ff848, "mount_bdev" },
	{ 0x3213f038, "mutex_unlock" },
	{ 0xa4f0837, "iomap_writepages" },
	{ 0x9f574b76, "alloc_pages" },
	{ 0xc00bd668, "inc_nlink" },
	{ 0x44e9a829, "match_token" },
	{ 0xc15c7dd2, "touch_atime" },
	{ 0x5ce7b7e6, "blk_op_str" },
	{ 0xa892b7a9, "__percpu_down_read" },
	{ 0xe82fb61f, "generic_file_open" },
	{ 0x9d0dfb2f, "blkdev_issue_flush" },
	{ 0x139e904b, "seq_puts" },
	{ 0xebbc1efb, "rcuwait_wake_up" },
	{ 0x53b612f, "send_sig" },
	{ 0x71a29dfc, "filemap_splice_read" },
	{ 0x817f91ba, "kmalloc_trace" },
	{ 0x60a13e90, "rcu_barrier" },
	{ 0xd6f2422c, "generic_file_llseek_size" },
	{ 0x7aa1756e, "kvfree" },
	{ 0x688e72e1, "__SCT__preempt_schedule_notrace" },
	{ 0xba555ceb, "kmem_cache_alloc_lru" },
	{ 0xe53d0a71, "filemap_fault" },
	{ 0xb5b54b34, "_raw_spin_unlock" },
	{ 0x837f15f1, "generic_file_llseek" },
	{ 0xe3bb10b6, "iomap_release_folio" },
	{ 0x4d23dfc3, "iomap_is_partially_uptodate" },
	{ 0x7381287f, "trace_handle_return" },
	{ 0x53b954a2, "up_read" },
	{ 0x14b8ea0b, "submit_bio_wait" },
	{ 0x886939d0, "blkdev_report_zones" },
	{ 0xe2c17b5d, "__SCT__might_resched" },
	{ 0x69ef14bf, "kmalloc_caches" },
	{ 0x2ea79c03, "kmem_cache_destroy" },
	{ 0xd85d1c40, "kobject_put" },
	{ 0xbc314156, "nop_mnt_idmap" },
	{ 0x7d196a5f, "module_layout" },
};

MODULE_INFO(depends, "");


MODULE_INFO(srcversion, "1338794ABD24F430DB7D175");
MODULE_INFO(rhelversion, "9.7");
