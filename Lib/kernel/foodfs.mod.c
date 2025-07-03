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
	{ 0xbdfb6dbb, "__fentry__" },
	{ 0x5b8239ca, "__x86_return_thunk" },
	{ 0xe1f1d2ed, "new_inode" },
	{ 0x92997ed8, "_printk" },
	{ 0x88db9f48, "__check_object_size" },
	{ 0x13c49cc2, "_copy_from_user" },
	{ 0x3705c556, "simple_dentry_operations" },
	{ 0x6ac27df0, "current_time" },
	{ 0x4ca6cc0c, "simple_dir_inode_operations" },
	{ 0x404fae36, "simple_dir_operations" },
	{ 0x6e4000fa, "set_nlink" },
	{ 0x8800f2c7, "d_make_root" },
	{ 0x65932bda, "mount_nodev" },
	{ 0x639a8065, "kill_litter_super" },
	{ 0x3327706d, "kmem_cache_create" },
	{ 0x61338c3b, "register_filesystem" },
	{ 0x60a13e90, "rcu_barrier" },
	{ 0x2ea79c03, "kmem_cache_destroy" },
	{ 0x6b10bee1, "_copy_to_user" },
	{ 0x392616f2, "unregister_filesystem" },
	{ 0xfa6a3648, "simple_statfs" },
	{ 0x7d196a5f, "module_layout" },
};

MODULE_INFO(depends, "");


MODULE_INFO(srcversion, "02A370C9C66F7B29777BDE9");
MODULE_INFO(rhelversion, "9.7");
