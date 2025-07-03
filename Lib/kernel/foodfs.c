#include "foodfs.h"




static int __init foodfs_init_inodecache(void)
{
	foodfs_inode_cachep = kmem_cache_create("zonefs_inode_cache",
			sizeof(struct foodfs_inode_info), 0,
			(SLAB_RECLAIM_ACCOUNT | SLAB_MEM_SPREAD | SLAB_ACCOUNT),
			NULL);
	if (foodfs_inode_cachep == NULL)
		return -ENOMEM;
	return 0;
}

static void foodfs_destroy_inodecache(void)
{
	/*
	 * Make sure all delayed rcu free inodes are flushed before we
	 * destroy the inode cache.
	 */
	rcu_barrier();
	kmem_cache_destroy(foodfs_inode_cachep);
}


int foodfs_create (struct mnt_idmap *mntid, struct inode *dir, struct dentry *dentry, umode_t mode, bool excl) {
    struct super_block *sb = dir->i_sb;
    struct inode *inode = new_inode(sb);
    inode->i_sb = sb;
    inode->i_fop = &foodfs_file_ops; 
    inode->i_private = block; // Initialize private data pointer
    inode->i_mode = mode;
    inode->i_op = &foodfs_inode_ops; // Set the inode operations for the
    return 0;
};

int foodfs_mkdir (struct mnt_idmap *mntid, struct inode*, struct dentry*, umode_t) {

    printk(KERN_INFO "FoodFS: Creating directory\n");
    return 0;
};

int foodfs_rmdir (struct inode*, struct dentry*) {

    printk(KERN_INFO "FoodFS: Removing directory\n");
    return 0;
};



ssize_t foodfs_write (struct file *file, const char __user *buf, size_t count, loff_t *pos) {
    // Implement write operation
    copy_from_user(buf, block, count); // Copy data from user space to block
    return 0; // Return number of bytes written
}

ssize_t foodfs_read(struct file *file, char __user *buf, size_t count, loff_t *pos) {
    // Implement read operation
    copy_to_user(block, buf, count); // Copy data from block to user space
    return 0; // Return number of bytes read
}

int foodfs_open(struct inode *inode, struct file *file) {
    // Implement open operation
    file->private_data = block; // Set private data pointer to block
    return 0; // Return 0 on success
}

int foodfs_release (struct inode *ind, struct file *f) {
    // Implement release operation
    printk(KERN_INFO "FoodFS: File released\n");
    f->private_data = NULL; // Clear private data pointer
    return 0; // Return 0 on success
}


// here may be the question that cause core dump
int foodfs_fill_super(struct super_block *s, void *data, int silent) {
	struct inode *inode;
	int error;

	s->s_iflags &= ~SB_I_NODEV;
	s->s_blocksize = 1024;
	s->s_blocksize_bits = 10;
	s->s_magic = FOODFS_MAGIC;
	s->s_op = &foodfs_sops;
	s->s_d_op = &simple_dentry_operations;
	s->s_time_gran = 1;

	error = -ENOMEM;
	s->s_fs_info = block; // new_pts_fs_info(s);
	if (!s->s_fs_info)
		goto fail;

	// error = parse_mount_options(data, PARSE_MOUNT, &DEVPTS_SB(s)->mount_opts);
	// if (error)
	// 	goto fail;

	error = -ENOMEM;
	inode = new_inode(s);
	if (!inode)
		goto fail;
	inode->i_ino = 1;
	inode->i_mtime = inode->i_atime = inode->i_ctime = current_time(inode);
	inode->i_mode = S_IFDIR | S_IRUGO | S_IXUGO | S_IWUSR;
	inode->i_op = &simple_dir_inode_operations;
	inode->i_fop = &simple_dir_operations;
	set_nlink(inode, 2);

	s->s_root = d_make_root(inode);
	if (!s->s_root) {
		pr_err("get root dentry failed\n");
		goto fail;
	}

	// error = mknod_ptmx(s);
	// if (error)
	// 	goto fail_dput;
    printk(KERN_INFO "FoodFS: Superblock initialized\n");
	return 0;
fail_dput:
	dput(s->s_root);
	s->s_root = NULL;
fail:
	return error;

    printk(KERN_INFO "FoodFS: Superblock initialized\n");
    return 0; // Return 0 on success
}

int food_init_fs_context(struct fs_context *fc) {


	return 0;
}

struct dentry* food_mount(struct file_system_type *fs_type, int flags, const char *dev_name, void *data) {
    
    // struct file_system_type *fs = fs_type;

    printk(KERN_INFO "FoodFS: Mounting filesystem\n");
    printk(KERN_INFO "FoodFS: dev_name: %s\n", dev_name);
    printk(KERN_INFO "FoodFS: flags: %d\n", flags);

    return mount_nodev(fs_type, flags, data, foodfs_fill_super);


}

void food_kill_sb(struct super_block* sb) {
    printk(KERN_INFO "FoodFS: Unmounting filesystem\n");
    
    kill_litter_super(sb); // Clean up the superblock
    return;
}





static int __init foodfs_init(void) {

    int ret = 0;
    
    // BUILD_BUG_ON(sizeof(struct foodfs_super) != ZONEFS_SUPER_SIZE);

	ret = foodfs_init_inodecache();
	if (ret) {
		return ret;
    }

    ret = register_filesystem(&foodfs);
    if(ret) {
        printk(KERN_ERR "FoodFS: Failed to register filesystem: %d\n", ret);
        foodfs_destroy_inodecache();
        return ret;
    }
    printk(KERN_INFO "FoodFS: Filesystem registered, foodfs addr:%p\n", &foodfs);
    return 0;

}

static void __exit foodfs_exit(void) {
    
    int ret = 0;
    foodfs_destroy_inodecache();

    ret = unregister_filesystem(&foodfs);
    if(ret) {
        printk(KERN_ERR "FoodFS: Failed to unregister filesystem: %d\n", ret);
        return;
    }
    printk(KERN_INFO "FoodFS: Filesystem unregistered\n");
    return;
}

module_init(foodfs_init);
module_exit(foodfs_exit);
MODULE_LICENSE("GPL");
